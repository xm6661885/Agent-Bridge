package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"agent-bridge/config"
	"agent-bridge/core"
)

func runSend(args []string) {
	req, dataDir, err := parseSendArgs(args)
	if err != nil {
		if errors.Is(err, errSendUsage) {
			printSendUsage()
			return
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		printSendUsage()
		os.Exit(1)
	}

	sockPath := resolveSocketPath(dataDir)
	if _, err := os.Stat(sockPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: agent-bridge is not running (socket not found: %s)\n", sockPath)
		os.Exit(1)
	}

	payload, err := buildSendPayload(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to encode send payload: %v\n", err)
		os.Exit(1)
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", sockPath)
			},
		},
	}

	resp, err := client.Post("http://unix/send", "application/json", bytes.NewReader(payload))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Error: %s\n", strings.TrimSpace(string(body)))
		os.Exit(1)
	}

	fmt.Println("Sent successfully.")
}

var errSendUsage = errors.New("show send usage")

func parseSendArgs(args []string) (core.SendRequest, string, error) {
	var req core.SendRequest
	var dataDir string
	var imagePaths []string
	var filePaths []string
	var audioPaths []string
	var videoPaths []string

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--project", "-p":
			if i+1 >= len(args) {
				return req, "", fmt.Errorf("--project requires a value")
			}
			i++
			req.Project = args[i]
		case "--session", "-s":
			if i+1 >= len(args) {
				return req, "", fmt.Errorf("--session requires a value")
			}
			i++
			req.SessionKey = args[i]
		case "--message", "-m":
			if i+1 >= len(args) {
				return req, "", fmt.Errorf("--message requires a value")
			}
			i++
			req.Message = args[i]
		case "--cwd", "--work-dir":
			if i+1 >= len(args) {
				return req, "", fmt.Errorf("%s requires a value", args[i])
			}
			i++
			req.WorkDir = args[i]
		case "--image":
			if i+1 >= len(args) {
				return req, "", fmt.Errorf("--image requires a path")
			}
			i++
			imagePaths = append(imagePaths, args[i])
		case "--file":
			if i+1 >= len(args) {
				return req, "", fmt.Errorf("--file requires a path")
			}
			i++
			filePaths = append(filePaths, args[i])
		case "--audio":
			if i+1 >= len(args) {
				return req, "", fmt.Errorf("--audio requires a path")
			}
			i++
			audioPaths = append(audioPaths, args[i])
		case "--video":
			if i+1 >= len(args) {
				return req, "", fmt.Errorf("--video requires a path")
			}
			i++
			videoPaths = append(videoPaths, args[i])
		case "--data-dir":
			if i+1 >= len(args) {
				return req, "", fmt.Errorf("--data-dir requires a value")
			}
			i++
			dataDir = args[i]
		case "--help", "-h":
			return req, "", errSendUsage
		default:
			return req, "", fmt.Errorf("unexpected argument %q: text-only sends are not supported; use -m together with --image/--file", args[i])
		}
	}

	if req.Project == "" {
		req.Project = strings.TrimSpace(os.Getenv("AGENT_BRIDGE_PROJECT"))
	}
	if req.SessionKey == "" {
		req.SessionKey = strings.TrimSpace(os.Getenv("AGENT_BRIDGE_SESSION_KEY"))
	}
	maxAtt := resolveMaxAttachmentSize(loadSendConfigBestEffort())

	images, err := loadImageAttachments(imagePaths, maxAtt)
	if err != nil {
		return req, "", err
	}
	files, err := loadFileAttachments(filePaths, maxAtt)
	if err != nil {
		return req, "", err
	}
	audioFiles, err := loadTypedFileAttachments(audioPaths, "audio", maxAtt)
	if err != nil {
		return req, "", err
	}
	videoFiles, err := loadTypedFileAttachments(videoPaths, "video", maxAtt)
	if err != nil {
		return req, "", err
	}
	req.Images = images
	// Keep audio / video clips on dedicated fields. Routing them through
	// req.Files would force the engine to dispatch them via FileSender —
	// that loses the native voice-bubble / video-bubble path on platforms
	// that implement AudioSender / VideoSender (e.g. an ffmpeg
	// transcode for mp3 → opus). See agent-bridge internal task
	// t-20260615-cqjbk1.
	req.Files = files
	req.Audios = audioFiles
	req.Videos = videoFiles

	if len(req.Images) == 0 && len(req.Files) == 0 && len(req.Audios) == 0 && len(req.Videos) == 0 {
		return req, "", fmt.Errorf("an attachment (--image/--file/--audio/--video) is required")
	}
	if req.Message != "" && len(req.Images) == 0 && len(req.Files) == 0 {
		return req, "", fmt.Errorf("--message must accompany --image or --file; plain text sends are not supported")
	}

	return req, dataDir, nil
}

func loadImageAttachments(paths []string, maxSize int64) ([]core.ImageAttachment, error) {
	images := make([]core.ImageAttachment, 0, len(paths))
	for _, path := range paths {
		data, fileName, mimeType, err := readAttachment(path, maxSize)
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(mimeType, "image/") {
			return nil, fmt.Errorf("%s is not an image (detected mime: %s)", path, mimeType)
		}
		images = append(images, core.ImageAttachment{MimeType: mimeType, Data: data, FileName: fileName})
	}
	return images, nil
}

func loadFileAttachments(paths []string, maxSize int64) ([]core.FileAttachment, error) {
	files := make([]core.FileAttachment, 0, len(paths))
	for _, path := range paths {
		data, fileName, mimeType, err := readAttachment(path, maxSize)
		if err != nil {
			return nil, err
		}
		files = append(files, core.FileAttachment{MimeType: mimeType, Data: data, FileName: fileName})
	}
	return files, nil
}

func loadTypedFileAttachments(paths []string, mediaType string, maxSize int64) ([]core.FileAttachment, error) {
	files := make([]core.FileAttachment, 0, len(paths))
	for _, path := range paths {
		data, fileName, mimeType, err := readAttachment(path, maxSize)
		if err != nil {
			return nil, err
		}
		if !attachmentMatchesMediaType(mimeType, fileName, mediaType) {
			return nil, fmt.Errorf("%s is not %s media (detected mime: %s)", path, mediaType, mimeType)
		}
		files = append(files, core.FileAttachment{MimeType: mimeType, Data: data, FileName: fileName})
	}
	return files, nil
}

func attachmentMatchesMediaType(mimeType, fileName, mediaType string) bool {
	ext := strings.ToLower(filepath.Ext(fileName))
	switch mediaType {
	case "audio":
		if strings.HasPrefix(mimeType, "audio/") {
			return true
		}
		switch ext {
		case ".aac", ".flac", ".m4a", ".mp3", ".oga", ".ogg", ".opus", ".wav":
			return true
		}
	case "video":
		if strings.HasPrefix(mimeType, "video/") {
			return true
		}
		switch ext {
		case ".avi", ".m4v", ".mkv", ".mov", ".mp4", ".webm":
			return true
		}
	}
	return false
}

// loadSendConfigBestEffort loads config.toml — resolved the same way the
// daemon resolves it — so the standalone `agent-bridge send` subcommand (a
// separate process that otherwise has no config) can honour
// max_attachment_size_mb. Errors are ignored and nil is returned, so a missing
// or invalid config never breaks sending; the caller then falls back to the
// env var / default via resolveMaxAttachmentSize.
func loadSendConfigBestEffort() *config.Config {
	cfg, err := config.Load(resolveConfigPath(""))
	if err != nil {
		return nil
	}
	return cfg
}

// readAttachment reads a single attachment file, rejecting anything larger than
// maxSize bytes (resolved by the caller from config/env/default). The limit is
// enforced before the file is read into memory.
func readAttachment(path string, maxSize int64) ([]byte, string, string, error) {
	cleaned := filepath.Clean(path)

	info, err := os.Stat(cleaned)
	if err != nil {
		return nil, "", "", fmt.Errorf("read attachment %s: %w", path, err)
	}
	if info.Size() > maxSize {
		return nil, "", "", fmt.Errorf("attachment %s exceeds size limit (%d MB)", path, maxSize>>20)
	}

	data, err := os.ReadFile(cleaned)
	if err != nil {
		return nil, "", "", fmt.Errorf("read attachment %s: %w", path, err)
	}
	fileName := filepath.Base(cleaned)
	return data, fileName, detectAttachmentMimeType(fileName, data), nil
}

func detectAttachmentMimeType(fileName string, data []byte) string {
	ext := strings.ToLower(filepath.Ext(fileName))
	switch ext {
	case ".md", ".markdown":
		return "text/markdown; charset=utf-8"
	}
	if byExt := mime.TypeByExtension(ext); byExt != "" {
		return byExt
	}
	if len(data) == 0 {
		return "application/octet-stream"
	}
	sniff := data
	if len(sniff) > 512 {
		sniff = sniff[:512]
	}
	return http.DetectContentType(sniff)
}

func buildSendPayload(req core.SendRequest) ([]byte, error) {
	return json.Marshal(req)
}

func resolveSocketPath(dataDir string) string {
	if dataDir != "" {
		return filepath.Join(dataDir, "run", "api.sock")
	}
	// Check AGENT_BRIDGE_DATA_DIR env var for custom data_dir configuration
	if envDataDir := strings.TrimSpace(os.Getenv("AGENT_BRIDGE_DATA_DIR")); envDataDir != "" {
		return filepath.Join(envDataDir, "run", "api.sock")
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".agent-bridge", "run", "api.sock")
	}
	return filepath.Join(".agent-bridge", "run", "api.sock")
}

func printSendUsage() {
	fmt.Println(`Usage: agent-bridge send [options] --image <path>
       agent-bridge send [options] --file <path>
       agent-bridge send [options] --audio <path>
       agent-bridge send [options] --video <path>

Send attachments to an active agent-bridge session.
Text-only messages are not supported; reply with text directly instead.

Options:
  -m, --message <text>     Caption sent with --image/--file (not allowed alone)
      --image <path>       Send an image attachment (repeatable)
      --file <path>        Send a file attachment (repeatable)
      --audio <path>       Send an audio attachment (repeatable)
      --video <path>       Send a video attachment (repeatable)
      --cwd <path>         Start a new session in this working directory
      --work-dir <path>    Alias for --cwd
  -p, --project <name>     Target project (optional if only one project)
  -s, --session <key>      Target session key (optional, picks first active)
      --data-dir <path>    Data directory (default: ~/.agent-bridge)
  -h, --help               Show this help

Examples:
  agent-bridge send --image /tmp/chart.png
  agent-bridge send -m "Chart generated" --image /tmp/chart.png
  agent-bridge send --file /tmp/report.pdf
  agent-bridge send --video /tmp/demo.mp4
  agent-bridge send --audio /tmp/voice.opus`)
}
