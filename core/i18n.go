package core

import "fmt"

// I18n provides the user-facing message catalog. agent-bridge is
// English-only; the type is kept so call sites stay uniform.
type I18n struct{}

func NewI18n() *I18n {
	return &I18n{}
}

// Message keys
type MsgKey string

const (
	MsgStarting                  MsgKey = "starting"
	MsgThinking                  MsgKey = "thinking"
	MsgTool                      MsgKey = "tool"
	MsgToolResult                MsgKey = "tool_result"
	MsgToolResultFmtStatus       MsgKey = "tool_result_fmt_status"
	MsgToolResultFmtExit         MsgKey = "tool_result_fmt_exit"
	MsgToolResultFmtNoOutput     MsgKey = "tool_result_fmt_no_output"
	MsgToolResultFmtOk           MsgKey = "tool_result_fmt_ok"
	MsgToolResultFmtFailed       MsgKey = "tool_result_fmt_failed"
	MsgExecutionStopped          MsgKey = "execution_stopped"
	MsgSessionCloseFailed        MsgKey = "session_close_failed"
	MsgSessionResumeUnsafe       MsgKey = "session_resume_unsafe"
	MsgSessionCancelled          MsgKey = "session_cancelled"
	MsgNoExecution               MsgKey = "no_execution"
	MsgPreviousProcessing        MsgKey = "previous_processing"
	MsgSteering                  MsgKey = "steering"
	MsgQueueFull                 MsgKey = "queue_full"
	MsgMessageQueued             MsgKey = "message_queued"
	MsgAttachmentStaged          MsgKey = "attachment_staged"
	MsgNoToolsAllowed            MsgKey = "no_tools_allowed"
	MsgCurrentTools              MsgKey = "current_tools"
	MsgCurrentSession            MsgKey = "current_session"
	MsgToolAuthNotSupported      MsgKey = "tool_auth_not_supported"
	MsgToolAllowFailed           MsgKey = "tool_allow_failed"
	MsgToolAllowedNew            MsgKey = "tool_allowed_new"
	MsgError                     MsgKey = "error"
	MsgSessionNotFound           MsgKey = "session_not_found"
	MsgFailedToStartAgentSession MsgKey = "failed_to_start_agent_session"
	MsgFailedToDeleteSession     MsgKey = "failed_to_delete_session"
	MsgEmptyResponse             MsgKey = "empty_response"
	MsgPermissionPrompt          MsgKey = "permission_prompt"
	MsgPermissionAllowed         MsgKey = "permission_allowed"
	MsgPermissionApproveAll      MsgKey = "permission_approve_all"
	MsgPermissionDenied          MsgKey = "permission_denied_msg"
	MsgPermissionHint            MsgKey = "permission_hint"
	MsgQuietOn                   MsgKey = "quiet_on"
	MsgQuietOff                  MsgKey = "quiet_off"
	MsgDisplayModeCompact        MsgKey = "display_mode_compact"
	MsgModeChanged               MsgKey = "mode_changed"
	MsgModeNotSupported          MsgKey = "mode_not_supported"
	MsgSessionNotStarted         MsgKey = "session_not_started"
	MsgUntitled                  MsgKey = "untitled"
	MsgWelcome                   MsgKey = "welcome"
	MsgHelp                      MsgKey = "message_help" // change from "help", which is used now for builtin command help
	MsgHelpTitle                 MsgKey = "help_title"
	MsgHelpSessionSection        MsgKey = "help_session_section"
	MsgHelpAgentSection          MsgKey = "help_agent_section"
	MsgHelpToolsSection          MsgKey = "help_tools_section"
	MsgHelpSystemSection         MsgKey = "help_system_section"
	MsgHelpTip                   MsgKey = "help_tip"
	MsgListTitle                 MsgKey = "list_title"
	MsgListTitlePaged            MsgKey = "list_title_paged"
	MsgListEmpty                 MsgKey = "list_empty"
	MsgListPageHint              MsgKey = "list_page_hint"
	MsgListSwitchHint            MsgKey = "list_switch_hint"
	MsgListError                 MsgKey = "list_error"
	MsgHistoryEmpty              MsgKey = "history_empty"
	MsgNameUsage                 MsgKey = "name_usage"
	MsgNameSet                   MsgKey = "name_set"
	MsgNameNoSession             MsgKey = "name_no_session"
	MsgProviderNotSupported      MsgKey = "provider_not_supported"
	MsgProviderNone              MsgKey = "provider_none"
	MsgProviderCurrent           MsgKey = "provider_current"
	MsgProviderListTitle         MsgKey = "provider_list_title"
	MsgProviderListEmpty         MsgKey = "provider_list_empty"
	MsgProviderSwitchHint        MsgKey = "provider_switch_hint"
	MsgProviderNotFound          MsgKey = "provider_not_found"
	MsgProviderSwitched          MsgKey = "provider_switched"
	MsgProviderCleared           MsgKey = "provider_cleared"
	MsgProviderAdded             MsgKey = "provider_added"
	MsgProviderAddUsage          MsgKey = "provider_add_usage"
	MsgProviderAddFailed         MsgKey = "provider_add_failed"
	MsgProviderRemoved           MsgKey = "provider_removed"
	MsgCardTitleProviderAdd      MsgKey = "card_title_provider_add"
	MsgProviderAddPickHint       MsgKey = "provider_add_pick_hint"
	MsgProviderAddOther          MsgKey = "provider_add_other"
	MsgProviderLinkGlobal        MsgKey = "provider_link_global"

	MsgVoiceNotEnabled               MsgKey = "voice_not_enabled"
	MsgVoiceUsingPlatformRecognition MsgKey = "voice_using_platform_recognition"
	MsgVoiceNoFFmpeg                 MsgKey = "voice_no_ffmpeg"
	MsgVoiceTranscribing             MsgKey = "voice_transcribing"
	MsgVoiceTranscribed              MsgKey = "voice_transcribed"
	MsgVoiceTranscribeFailed         MsgKey = "voice_transcribe_failed"
	MsgVoiceEmpty                    MsgKey = "voice_empty"

	MsgTTSNotEnabled MsgKey = "tts_not_enabled"
	MsgTTSStatus     MsgKey = "tts_status"
	MsgTTSSwitched   MsgKey = "tts_switched"
	MsgTTSUsage      MsgKey = "tts_usage"

	MsgReplyFooterRemaining  MsgKey = "reply_footer_remaining"
	MsgModelCurrent          MsgKey = "model_current"
	MsgModelChanged          MsgKey = "model_changed"
	MsgModelChangeFailed     MsgKey = "model_change_failed"
	MsgModelCardSwitching    MsgKey = "model_card_switching"
	MsgModelCardSwitched     MsgKey = "model_card_switched"
	MsgModelCardSwitchFailed MsgKey = "model_card_switch_failed"
	MsgModelNotSupported     MsgKey = "model_not_supported"
	MsgReasoningCurrent      MsgKey = "reasoning_current"
	MsgReasoningChanged      MsgKey = "reasoning_changed"
	MsgReasoningNotSupported MsgKey = "reasoning_not_supported"

	MsgCompressNotSupported MsgKey = "compress_not_supported"
	MsgCompressing          MsgKey = "compressing"
	MsgCompressNoSession    MsgKey = "compress_no_session"
	MsgCompressDone         MsgKey = "compress_done"

	// Inline strings previously hardcoded in engine.go

	MsgModelDefault               MsgKey = "model_default"
	MsgModelListTitle             MsgKey = "model_list_title"
	MsgModelUsage                 MsgKey = "model_usage"
	MsgReasoningDefault           MsgKey = "reasoning_default"
	MsgReasoningListTitle         MsgKey = "reasoning_list_title"
	MsgReasoningUsage             MsgKey = "reasoning_usage"
	MsgReasoningSelectPlaceholder MsgKey = "reasoning_select_placeholder"

	MsgModeUsage                 MsgKey = "mode_usage"
	MsgModelSelectPlaceholder    MsgKey = "model_select_placeholder"
	MsgModeSelectPlaceholder     MsgKey = "mode_select_placeholder"
	MsgProviderSelectPlaceholder MsgKey = "provider_select_placeholder"
	MsgProviderClearOption       MsgKey = "provider_clear_option"
	MsgCardBack                  MsgKey = "card_back"
	MsgCardPrev                  MsgKey = "card_prev"
	MsgCardNext                  MsgKey = "card_next"
	MsgCardTitleModel            MsgKey = "card_title_model"
	MsgCardTitleReasoning        MsgKey = "card_title_reasoning"
	MsgCardTitleMode             MsgKey = "card_title_mode"
	MsgCardTitleSessions         MsgKey = "card_title_sessions"
	MsgCardTitleSessionsPaged    MsgKey = "card_title_sessions_paged"
	MsgCardTitleCurrentSession   MsgKey = "card_title_current_session"
	MsgCardTitleHistory          MsgKey = "card_title_history"
	MsgCardTitleHistoryLast      MsgKey = "card_title_history_last"
	MsgCardTitleProvider         MsgKey = "card_title_provider"
	MsgCardTitleCommands         MsgKey = "card_title_commands"
	MsgCardTitleAlias            MsgKey = "card_title_alias"
	MsgListItem                  MsgKey = "list_item"
	MsgListEmptySummary          MsgKey = "list_empty_summary"
	MsgCommandsTagAgent          MsgKey = "commands_tag_agent"
	MsgCommandsTagShell          MsgKey = "commands_tag_shell"

	MsgPermBtnAllow    MsgKey = "perm_btn_allow"
	MsgPermBtnDeny     MsgKey = "perm_btn_deny"
	MsgPermBtnAllowAll MsgKey = "perm_btn_allow_all"
	MsgPermCardTitle   MsgKey = "perm_card_title"
	MsgPermCardBody    MsgKey = "perm_card_body"
	MsgPermCardNote    MsgKey = "perm_card_note"

	MsgAskQuestionTitle     MsgKey = "ask_question_title"
	MsgAskQuestionNote      MsgKey = "ask_question_note"
	MsgAskQuestionNoteMulti MsgKey = "ask_question_note_multi"
	MsgAskQuestionMulti     MsgKey = "ask_question_multi"

	MsgAskQuestionDone        MsgKey = "ask_question_done"
	MsgAskQuestionNoteToggle  MsgKey = "ask_question_note_toggle"
	MsgAskQuestionNoteOther   MsgKey = "ask_question_note_other"
	MsgAskQuestionReplySingle MsgKey = "ask_question_reply_single"
	MsgAskQuestionReplyFree   MsgKey = "ask_question_reply_free"
	MsgInteractionNotOwner    MsgKey = "interaction_not_owner"

	MsgPlanTitle             MsgKey = "plan_title"
	MsgPlanOptAutoEdits      MsgKey = "plan_opt_auto_edits"
	MsgPlanOptReview         MsgKey = "plan_opt_review"
	MsgPlanOptRevise         MsgKey = "plan_opt_revise"
	MsgPlanReplyWith         MsgKey = "plan_reply_with"
	MsgPlanNote              MsgKey = "plan_note"
	MsgPlanAskFeedback       MsgKey = "plan_ask_feedback"
	MsgPlanApprovedAutoEdits MsgKey = "plan_approved_auto_edits"
	MsgPlanApprovedReview    MsgKey = "plan_approved_review"
	MsgPlanKeepPlanning      MsgKey = "plan_keep_planning"
	MsgPlanFeedbackSent      MsgKey = "plan_feedback_sent"
	MsgPlanOptBypass         MsgKey = "plan_opt_bypass"
	MsgPlanApprovedBypass    MsgKey = "plan_approved_bypass"
	MsgPlanBypassUnavailable MsgKey = "plan_bypass_unavailable"

	MsgCommandsTitle        MsgKey = "commands_title"
	MsgCommandsEmpty        MsgKey = "commands_empty"
	MsgCommandsHint         MsgKey = "commands_hint"
	MsgCommandsUsage        MsgKey = "commands_usage"
	MsgCommandsAddUsage     MsgKey = "commands_add_usage"
	MsgCommandsAddExecUsage MsgKey = "commands_addexec_usage"
	MsgCommandsAdded        MsgKey = "commands_added"
	MsgCommandsExecAdded    MsgKey = "commands_exec_added"
	MsgCommandsAddExists    MsgKey = "commands_add_exists"
	MsgCommandsDelUsage     MsgKey = "commands_del_usage"
	MsgCommandsDeleted      MsgKey = "commands_deleted"
	MsgCommandsNotFound     MsgKey = "commands_not_found"

	MsgRestarting     MsgKey = "restarting"
	MsgRestartSuccess MsgKey = "restart_success"

	MsgAliasEmpty      MsgKey = "alias_empty"
	MsgAliasListHeader MsgKey = "alias_list_header"
	MsgAliasAdded      MsgKey = "alias_added"
	MsgAliasDeleted    MsgKey = "alias_deleted"
	MsgAliasNotFound   MsgKey = "alias_not_found"
	MsgAliasUsage      MsgKey = "alias_usage"

	MsgNewSessionCreated      MsgKey = "new_session_created"
	MsgNewSessionCreatedName  MsgKey = "new_session_created_name"
	MsgSessionAutoResetIdle   MsgKey = "session_auto_reset_idle"
	MsgSessionClosingGraceful MsgKey = "session_closing_graceful"

	MsgDeleteUsage              MsgKey = "delete_usage"
	MsgDeleteSuccess            MsgKey = "delete_success"
	MsgDeleteActiveDenied       MsgKey = "delete_active_denied"
	MsgDeleteNotSupported       MsgKey = "delete_not_supported"
	MsgDeleteModeTitle          MsgKey = "delete_mode_title"
	MsgDeleteModeSelect         MsgKey = "delete_mode_select"
	MsgDeleteModeSelected       MsgKey = "delete_mode_selected"
	MsgDeleteModeSelectedCount  MsgKey = "delete_mode_selected_count"
	MsgDeleteModeDeleteSelected MsgKey = "delete_mode_delete_selected"
	MsgDeleteModeCancel         MsgKey = "delete_mode_cancel"
	MsgDeleteModeConfirmTitle   MsgKey = "delete_mode_confirm_title"
	MsgDeleteModeConfirmButton  MsgKey = "delete_mode_confirm_button"
	MsgDeleteModeBackButton     MsgKey = "delete_mode_back_button"
	MsgDeleteModeEmptySelection MsgKey = "delete_mode_empty_selection"
	MsgDeleteModeResultTitle    MsgKey = "delete_mode_result_title"
	MsgDeleteModeDeletingTitle  MsgKey = "delete_mode_deleting_title"
	MsgDeleteModeDeletingBody   MsgKey = "delete_mode_deleting_body"
	MsgDeleteModeMissingSession MsgKey = "delete_mode_missing_session"

	MsgSwitchSuccess   MsgKey = "switch_success"
	MsgSwitchNoMatch   MsgKey = "switch_no_match"
	MsgSwitchNoSession MsgKey = "switch_no_session"

	MsgCommandTimeout MsgKey = "command_timeout"

	MsgBannedWordBlocked MsgKey = "banned_word_blocked"
	MsgCommandDisabled   MsgKey = "command_disabled"
	MsgAdminRequired     MsgKey = "admin_required"
	MsgRateLimited       MsgKey = "rate_limited"

	MsgWhoamiTitle     MsgKey = "whoami_title"
	MsgWhoamiCardTitle MsgKey = "whoami_card_title"
	MsgWhoamiName      MsgKey = "whoami_name"
	MsgWhoamiPlatform  MsgKey = "whoami_platform"
	MsgWhoamiUsage     MsgKey = "whoami_usage"

	MsgSearchUsage    MsgKey = "search_usage"
	MsgSearchError    MsgKey = "search_error"
	MsgSearchNoResult MsgKey = "search_no_result"
	MsgSearchResult   MsgKey = "search_result"
	MsgSearchHint     MsgKey = "search_hint"

	MsgDiffEmpty       MsgKey = "diff_empty"
	MsgDiffNoDiff2HTML MsgKey = "diff_no_diff2html"

	MsgDirChanged          MsgKey = "dir_changed"
	MsgDirCurrent          MsgKey = "dir_current"
	MsgDirReset            MsgKey = "dir_reset"
	MsgDirUsage            MsgKey = "dir_usage"
	MsgDirNotSupported     MsgKey = "dir_not_supported"
	MsgDirInvalidPath      MsgKey = "dir_invalid_path"
	MsgDirHistoryTitle     MsgKey = "dir_history_title"
	MsgDirHistoryHint      MsgKey = "dir_history_hint"
	MsgDirInvalidIndex     MsgKey = "dir_invalid_index"
	MsgDirNoHistory        MsgKey = "dir_no_history"
	MsgDirNoPrevious       MsgKey = "dir_no_previous"
	MsgDirCardTitle        MsgKey = "dir_card_title"
	MsgDirCardPageHint     MsgKey = "dir_card_page_hint"
	MsgDirCardEmptyHistory MsgKey = "dir_card_empty_history"
	MsgDirCardReset        MsgKey = "dir_card_reset"
	MsgDirCardPrev         MsgKey = "dir_card_prev"
	MsgShowUsage           MsgKey = "show_usage"
	MsgShowParseError      MsgKey = "show_parse_error"
	MsgShowNotFound        MsgKey = "show_not_found"
	MsgShowDirWithLocation MsgKey = "show_dir_with_location"
	MsgShowReadFailed      MsgKey = "show_read_failed"

	// Multi-workspace messages
	MsgBackgroundAutoDenied MsgKey = "background_auto_denied"
)

var messages = map[MsgKey]string{
	MsgStarting:                  "Processing...",
	MsgThinking:                  "Thinking: %s",
	MsgTool:                      "**Tool #%d: %s**\n---\n%s",
	MsgToolResult:                "**%s**\n---\n%s",
	MsgToolResultFmtStatus:       "Status",
	MsgToolResultFmtExit:         "Exit",
	MsgToolResultFmtNoOutput:     "No output",
	MsgToolResultFmtOk:           "ok",
	MsgToolResultFmtFailed:       "failed",
	MsgExecutionStopped:          "Execution stopped.",
	MsgSessionCloseFailed:        "Warning: the stopped session's background process could not be confirmed killed. It may still be running and using its old credentials.",
	MsgSessionResumeUnsafe:       "The previous process could not be confirmed stopped, so this conversation was NOT resumed — a brand-new session was started instead (earlier context is not carried over). This avoids two agents acting on the same conversation.",
	MsgSessionCancelled:          "Session cancelled. Ready for new instructions.",
	MsgNoExecution:               "No execution in progress.",
	MsgPreviousProcessing:        "Previous request still processing, please wait...",
	MsgSteering:                  "Steering the current task with your new message…",
	MsgMessageQueued:             "Message received — will process after the current task finishes.",
	MsgQueueFull:                 "Message queue is full (%d pending). Please wait for current tasks to complete.",
	MsgAttachmentStaged:          "Got it — send your message and I'll include this.",
	MsgNoToolsAllowed:            "No tools pre-allowed.\nUsage: `/allow <tool_name>`\nExample: `/allow Bash`",
	MsgCurrentTools:              "Pre-allowed tools: %s",
	MsgCurrentSession:            "Current session\nName: %s\nSession ID: %s\nLocal messages: %d",
	MsgToolAuthNotSupported:      "This agent does not support tool authorization.",
	MsgToolAllowFailed:           "Failed to allow tool: %v",
	MsgToolAllowedNew:            "Tool `%s` pre-allowed. Takes effect on next session.",
	MsgError:                     "Error: %v",
	MsgBackgroundAutoDenied:      "Background task requested permission for `%s` but was auto-denied (no active user turn). Send a message or use `/yolo` to approve future requests.",
	MsgSessionNotFound:           "Session expired. Use /new to start a fresh conversation.",
	MsgFailedToStartAgentSession: "Error: failed to start agent session",
	MsgFailedToDeleteSession:     "%s: %v",
	MsgEmptyResponse:             "(empty response)",
	MsgPermissionPrompt:          "**Permission Request**\n\nAgent wants to use **%s**:\n\n```\n%s\n```\n\nReply **1** allow · **2** deny · **3** allow all (skip future prompts this session). Add a reason after deny (e.g. \"deny, use rg instead\") to tell the agent what to do instead.",
	MsgPermissionAllowed:         "Allowed, continuing...",
	MsgPermissionApproveAll:      "All permissions auto-approved for this session.",
	MsgPermissionDenied:          "Denied. Agent will stop this tool use.",
	MsgPermissionHint:            "Waiting for permission response. Reply **1** allow · **2** deny · **3** allow all, or /stop to cancel.",
	MsgQuietOn:                   "Quiet mode ON — thinking and tool progress messages will be hidden.",
	MsgQuietOff:                  "Quiet mode OFF — thinking and tool progress messages will be shown.",
	MsgDisplayModeCompact:        "Compact mode — thinking/tool hidden, each text segment sent separately.",
	MsgModeChanged:               "Permission mode switched to **%s**. New sessions will use this mode.",
	MsgModeNotSupported:          "This agent does not support permission mode switching.",
	MsgSessionNotStarted:         "(new — not yet started)",
	MsgUntitled:                  "(untitled)",
	MsgWelcome:                   "Hi! I'm agent-bridge, bridging you to **%s**.\n\nJust send a message to chat with the agent. Type /help to see built-in commands.",
	MsgHelp: "Available Commands\n\n" +
		"/new [name]\n  Start a new session\n\n" +
		"/list\n  List agent sessions\n\n" +
		"/search <keyword>\n  Search sessions by name or ID\n\n" +
		"/switch <number>\n  Resume a session by its list number\n\n" +
		"/delete <number>|1,2,3|3-7|1,3-5,8\n  Delete sessions by list number(s)\n\n" +
		"/name [number] <text>\n  Name a session for easy identification\n\n" +
		"/current\n  Show current active session\n\n" +
		"/history [n]\n  Show last n messages (default 10)\n\n" +
		"/provider [list|add|remove|switch|clear]\n  Manage API providers\n\n" +
		"/allow <tool>\n  Pre-allow a tool (next session)\n\n" +
		"/model [switch <name>]\n  View/switch model\n\n" +
		"/effort [level]\n  View/switch reasoning effort\n\n" +
		"/mode [name]\n  View/switch permission mode\n\n" +
		"/compact\n  Compact conversation context\n\n" +
		"/tts [always|voice_only]\n  View/switch text-to-speech mode\n\n" +
		"/shell [--timeout <sec>] <command>\n  Run a shell command and return the output (! prefix shortcut: !cmd)\n\n" +
		"/show <ref>\n  View a file, directory, or code snippet by reference\n\n" +
		"/dir [path|reset]\n  Show, switch, or reset agent working directory\n\n" +
		"/stop\n  Stop current execution\n\n" +
		"/commands [add|del]\n  Manage custom slash commands\n\n" +
		"/alias [add|del]\n  Manage command aliases (e.g. 帮助 → /help)\n\n" +
		"/restart\n  Restart agent-bridge service\n\n" +
		"/whoami\n  Show your User ID (for allow_from / admin_from)\n\n" +
		"/help\n  Show this help\n\n" +
		"Tip: Commands support prefix matching, e.g. `/pro l` = `/provider list`, `/sw 2` = `/switch 2`.\n\n" +
		"Custom commands: define via `/commands add` or `[[commands]]` in config.toml.\n\n" +
		"Command aliases: use `/alias add <trigger> <command>` or `[[aliases]]` in config.toml.\n\n" +
		"Permission modes: default / edit / plan / yolo",
	MsgHelpTitle: "agent-bridge Help",
	MsgHelpSessionSection: "**Session Management**\n" +
		"/new [name] — Start a new session\n" +
		"/list — List agent sessions\n" +
		"/search <keyword> — Search sessions\n" +
		"/switch <number> — Resume a session\n" +
		"/delete <number>|1,2,3|3-7|1,3-5,8 — Delete session(s)\n" +
		"/name [number] <text> — Name a session\n" +
		"/current — Show active session\n" +
		"/history [n] — Show last n messages",
	MsgHelpAgentSection: "**Agent Configuration**\n" +
		"/model [switch <name>] — View/switch model\n" +
		"/mode [name] — View/switch permission mode\n" +
		"/provider [list|add|...] — Manage API providers\n" +
		"/allow <tool> — Pre-allow a tool",
	MsgHelpToolsSection: "**Tools & Automation**\n" +
		"/shell <command> — Run a shell command (! shortcut)\n" +
		"/show <ref> — View file / directory / snippet by reference\n" +
		"/dir [path|reset] — Show, switch, or reset work directory\n" +
		"/commands [add|del] — Custom commands\n" +
		"/alias [add|del] — Command aliases\n" +
		"/compact — Compact context\n" +
		"/stop — Stop current execution",
	MsgHelpSystemSection: "**System**\n" +
		"/whoami — Show your User ID\n" +
		"/restart — Restart service",
	MsgHelpTip:              "Tip: Commands support prefix matching, e.g. /pro l = /provider list",
	MsgListTitle:            "**%s Sessions** (%d)\n\n",
	MsgListTitlePaged:       "**%s Sessions** (%d) · Page %d/%d\n\n",
	MsgListEmpty:            "No sessions found for this project.",
	MsgListPageHint:         "\n\nPage %d/%d \n\n`/list <page>` for more\n",
	MsgListSwitchHint:       "\n`/switch <number>` to switch session",
	MsgListError:            "Failed to list sessions: %v",
	MsgHistoryEmpty:         "No history in current session.",
	MsgNameUsage:            "Usage:\n`/name <text>` — name the current session\n`/name <number> <text>` — name a session by list number",
	MsgNameSet:              "Session named: **%s** (%s)",
	MsgNameNoSession:        "No active session. Send a message first or switch to a session.",
	MsgProviderNotSupported: "This agent does not support provider switching.",
	MsgProviderNone:         "No provider configured. Using agent's default environment.\n\nAdd providers in `config.toml` or via `agent-bridge provider add`.",
	MsgProviderCurrent:      "Active provider: **%s**\n\nUse `/provider list` to see all, `/provider switch <name>` to switch.",
	MsgProviderListTitle:    "Providers\n\n",
	MsgProviderListEmpty:    "No providers configured.\n\nAdd providers in `config.toml` or via `agent-bridge provider add`.",
	MsgProviderSwitchHint:   "`/provider switch <name>` to switch | `/provider clear` to reset",
	MsgProviderNotFound:     "Provider %q not found. Use `/provider list` to see available providers.",
	MsgProviderSwitched:     "Provider switched to **%s**. New sessions will use this provider.",
	MsgProviderCleared:      "Provider cleared. New sessions will use the default provider.",
	MsgProviderAdded:        "Provider **%s** added.\n\nUse `/provider switch %s` to activate.",
	MsgProviderAddUsage: "Usage:\n\n" +
		"`/provider add <name> <api_key> [base_url] [model]`\n\n" +
		"Or JSON:\n" +
		"`/provider add {\"name\":\"my-provider\",\"api_key\":\"sk-xxx\",\"base_url\":\"https://...\",\"model\":\"...\"}`",
	MsgProviderAddFailed:             "Failed to add provider: %v",
	MsgProviderRemoved:               "Provider **%s** removed.",
	MsgCardTitleProviderAdd:          "Add Provider",
	MsgProviderAddPickHint:           "Choose **Other** to enter a provider manually, or link an existing global provider.",
	MsgProviderAddOther:              "Other (manual)",
	MsgProviderLinkGlobal:            "Link existing provider",
	MsgVoiceNotEnabled:               "Voice messages are not enabled. Please configure `[speech]` in config.toml.",
	MsgVoiceUsingPlatformRecognition: "Voice transcription not configured, using %s built-in recognition",
	MsgVoiceNoFFmpeg:                 "Voice message requires `ffmpeg` for format conversion. Please install ffmpeg.",
	MsgVoiceTranscribing:             "Transcribing voice message...",
	MsgVoiceTranscribed:              "[Voice] %s",
	MsgVoiceTranscribeFailed:         "Voice transcription failed: %v",
	MsgVoiceEmpty:                    "Voice message was empty or could not be recognized.",
	MsgTTSNotEnabled:                 "TTS is not enabled. Please configure `[tts]` in config.toml.",
	MsgTTSStatus:                     "TTS status: enabled=true, mode=%s, provider=%s",
	MsgTTSSwitched:                   "TTS mode switched to: %s",
	MsgTTSUsage:                      "Usage: /tts [always|voice_only]",
	MsgReplyFooterRemaining:          "%d%% left",
	MsgModelCurrent:                  "Current model: %s",
	MsgModelChanged:                  "Model switched to `%s`. This session and all future sessions will use it.",
	MsgModelChangeFailed:             "Failed to change model: %v",
	MsgModelCardSwitching:            "Switching model to `%s`...",
	MsgModelCardSwitched:             "Model switched to `%s`.",
	MsgModelCardSwitchFailed:         "Failed to switch model: %v",
	MsgModelNotSupported:             "This agent does not support model switching.",
	MsgReasoningCurrent:              "Current reasoning effort: %s",
	MsgReasoningChanged:              "Reasoning effort switched to `%s`. New sessions will use this setting.",
	MsgReasoningNotSupported:         "This agent does not support reasoning effort switching.",
	MsgCompressNotSupported:          "This agent does not support context compaction.",
	MsgCompressing:                   "Compacting context...",
	MsgCompressNoSession:             "No active session to compact. Send a message first.",
	MsgCompressDone:                  "Context compacted.",
	MsgModelDefault:                  "Current model: (not set, using agent default)\n",
	MsgModelListTitle:                "Available models:\n",
	MsgModelUsage:                    "Usage: `/model switch <number>` or `/model switch <model_name>`",
	MsgReasoningDefault:              "Current reasoning effort: (not set, using Codex default)\n",
	MsgReasoningListTitle:            "Available reasoning levels:\n",
	MsgReasoningUsage:                "Usage: `/effort <number>` or `/effort <low|medium|high|xhigh|max>`",
	MsgModeUsage:                     "\nUse `/mode <name>` to switch.\nAvailable: %s",
	MsgModelSelectPlaceholder:        "Select model",
	MsgReasoningSelectPlaceholder:    "Select reasoning level",
	MsgModeSelectPlaceholder:         "Select mode",
	MsgProviderSelectPlaceholder:     "Select provider",
	MsgProviderClearOption:           "Do not use provider",
	MsgCardBack:                      "← Back",
	MsgCardPrev:                      "← Prev",
	MsgCardNext:                      "Next →",
	MsgCardTitleModel:                "Model",
	MsgCardTitleReasoning:            "Reasoning",
	MsgCardTitleMode:                 "Permission Mode",
	MsgCardTitleSessions:             "%s Sessions (%d)",
	MsgCardTitleSessionsPaged:        "%s Sessions (%d) — %d/%d",
	MsgCardTitleCurrentSession:       "Current Session",
	MsgCardTitleHistory:              "History",
	MsgCardTitleHistoryLast:          "History (last %d)",
	MsgCardTitleProvider:             "Provider",
	MsgCardTitleCommands:             "Commands",
	MsgCardTitleAlias:                "Alias",
	MsgListItem:                      "%s **%d.** %s · **%d** msgs · %s",
	MsgListEmptySummary:              "(empty)",
	MsgCommandsTagAgent:              " [agent]",
	MsgCommandsTagShell:              " [shell]",
	MsgPermBtnAllow:                  "Allow",
	MsgPermBtnDeny:                   "Deny",
	MsgPermBtnAllowAll:               "Allow All (this session)",
	MsgPermCardTitle:                 "Permission Request",
	MsgPermCardBody:                  "Agent wants to use **%s**:\n\n```\n%s\n```",
	MsgPermCardNote:                  "If buttons are unresponsive, reply: allow / deny / allow all",
	MsgAskQuestionTitle:              "Agent Question",
	MsgAskQuestionNote:               "If buttons are unresponsive, reply with the option number (e.g. 1) or type your answer",
	MsgAskQuestionNoteMulti:          "Reply with comma-separated option numbers (e.g. 1,3) or type your answer",
	MsgAskQuestionMulti:              " (multiple selections allowed, separate with commas)",
	MsgAskQuestionDone:               "Done",
	MsgAskQuestionNoteToggle:         "Tap options to toggle them, then tap Done. Or reply with numbers (e.g. 1,3) or your own answer.",
	MsgAskQuestionNoteOther:          "Tap an option, or reply with your own answer.",
	MsgAskQuestionReplySingle:        "Reply with an option number (e.g. 1) or type your own answer. Send /stop to cancel.",
	MsgAskQuestionReplyFree:          "Reply with your answer. Send /stop to cancel.",
	MsgInteractionNotOwner:           "Only the user who started this task can answer this prompt.",
	MsgPlanTitle:                     "Plan ready for review",
	MsgPlanOptAutoEdits:              "Approve, auto-accept edits",
	MsgPlanOptReview:                 "Approve, confirm each edit",
	MsgPlanOptRevise:                 "Keep planning",
	MsgPlanReplyWith:                 "Reply with a number:",
	MsgPlanNote:                      "Or reply with feedback and Claude will revise the plan. Send /stop to cancel.",
	MsgPlanAskFeedback:               "What should Claude change? Reply with your feedback, or reply **skip** to let Claude keep refining on its own.",
	MsgPlanApprovedAutoEdits:         "Plan approved — edits will be auto-accepted.",
	MsgPlanApprovedReview:            "Plan approved — you will be asked before each edit.",
	MsgPlanKeepPlanning:              "Claude will keep planning.",
	MsgPlanFeedbackSent:              "Feedback sent — Claude will revise the plan.",
	MsgPlanOptBypass:                 "Approve, bypass all permissions",
	MsgPlanApprovedBypass:            "Plan approved — all permissions are bypassed for this session (questions and plan reviews still come to you).",
	MsgPlanBypassUnavailable:         "This agent cannot switch to bypass mode mid-session. Choose 1, 2 or 3.",
	MsgCommandsTitle:                 "**Custom Commands** (%d)\n\n",
	MsgCommandsEmpty:                 "No custom commands configured.\n\nUse `/commands add <name> <prompt>` or add `[[commands]]` in config.toml.",
	MsgCommandsHint:                  "Type `/<name> [args]` to use.\n`/commands add <name> <prompt>` to add prompt command\n`/commands addexec <name> <shell>` to add exec command\n`/commands del <name>` to remove",
	MsgCommandsUsage:                 "Usage:\n`/commands` — list all custom commands\n`/commands add <name> <prompt>` — add prompt command\n`/commands addexec <name> <shell>` — add exec command\n`/commands del <name>` — remove a command",
	MsgCommandsAddUsage:              "Usage: `/commands add <name> <prompt template>`\n\nExample: `/commands add finduser Search the database for user「{{1}}」`",
	MsgCommandsAddExecUsage:          "Usage: `/commands addexec <name> <shell command>`\n         `/commands addexec --work-dir <dir> <name> <shell command>`\n\nExamples:\n`/commands addexec push git push`\n`/commands addexec status git status {{args}}`",
	MsgCommandsAdded:                 "Command `/%s` added.\nPrompt: %s",
	MsgCommandsAddExists:             "Command `/%s` already exists. Remove it first with `/commands del %s`.",
	MsgCommandsDelUsage:              "Usage: `/commands del <name>`",
	MsgCommandsDeleted:               "Command `/%s` removed.",
	MsgCommandsNotFound:              "Command `/%s` not found. Use `/commands` to see available commands.",
	MsgCommandsExecAdded:             "Exec command `/%s` added.\nCommand: %s",
	MsgRestarting:                    "Restarting agent-bridge...",
	MsgRestartSuccess:                "agent-bridge restarted successfully.",
	MsgAliasEmpty:                    "No aliases configured. Use `/alias add <trigger> <command>` to create one.",
	MsgAliasListHeader:               "Aliases (%d)",
	MsgAliasAdded:                    "Alias added: %s → %s",
	MsgAliasDeleted:                  "Alias removed: %s",
	MsgAliasNotFound:                 "Alias `%s` not found.",
	MsgAliasUsage:                    "Usage:\n  `/alias` — list all aliases\n  `/alias add <trigger> <command>` — add alias\n  `/alias del <trigger>` — remove alias\n\nExample: `/alias add 帮助 /help`",
	MsgNewSessionCreated:             "New session created",
	MsgNewSessionCreatedName:         "New session created: **%s**",
	MsgSessionAutoResetIdle:          "Session auto-reset after %d minute(s) of inactivity.",
	MsgSessionClosingGraceful:        "Wrapping up your previous session (usually a few seconds, up to 2 minutes). Your new session will start automatically.",
	MsgDeleteUsage:                   "Usage: `/delete <number>` or `/delete 1,2,3` or `/delete 3-7` or `/delete 1,3-5,8`.\nUse `/list` to see session numbers.",
	MsgDeleteSuccess:                 "Session deleted: %s",
	MsgSwitchSuccess:                 "Switched to: %s (%s, %d msgs)",
	MsgSwitchNoMatch:                 "No session matching %q",
	MsgSwitchNoSession:               "No session #%d",
	MsgCommandTimeout:                "Command timed out (60s): `%s`",
	MsgDeleteActiveDenied:            "Cannot delete the currently active session. Switch to another session first.",
	MsgDeleteNotSupported:            "This agent does not support session deletion.",
	MsgDeleteModeTitle:               "Delete Sessions",
	MsgDeleteModeSelect:              "Select",
	MsgDeleteModeSelected:            "Selected",
	MsgDeleteModeSelectedCount:       "%d selected",
	MsgDeleteModeDeleteSelected:      "Delete Selected",
	MsgDeleteModeCancel:              "Cancel",
	MsgDeleteModeConfirmTitle:        "Confirm Delete",
	MsgDeleteModeConfirmButton:       "Confirm Delete",
	MsgDeleteModeBackButton:          "Back",
	MsgDeleteModeEmptySelection:      "Select at least one session.",
	MsgDeleteModeResultTitle:         "Delete Result",
	MsgDeleteModeDeletingTitle:       "Deleting Sessions...",
	MsgDeleteModeDeletingBody:        "Deleting %d session(s), please wait...",
	MsgDeleteModeMissingSession:      "Missing selected session: %s",
	MsgBannedWordBlocked:             "Your message was blocked because it contains a prohibited word.",
	MsgCommandDisabled:               "Command `%s` is disabled for this project.",
	MsgAdminRequired:                 "Command `%s` requires admin privilege. Set `admin_from` in config to authorize users.",
	MsgRateLimited:                   "You are sending messages too fast. Please wait a moment.",
	MsgWhoamiTitle:                   "**Your Identity**",
	MsgWhoamiCardTitle:               "Your Identity",
	MsgWhoamiName:                    "Name",
	MsgWhoamiPlatform:                "Platform",
	MsgWhoamiUsage:                   "Use the `User ID` above for `allow_from` and `admin_from` in your `config.toml`.",
	MsgSearchUsage:                   "Usage: /search <keyword>\nSearch sessions by name or ID.",
	MsgSearchError:                   "Search error: %v",
	MsgSearchNoResult:                "No sessions found matching %q",
	MsgSearchResult:                  "Found %d session(s) matching %q:",
	MsgSearchHint:                    "Use /switch <id> to switch to a session.",
	MsgDiffEmpty:                     "No diff — clean working tree (or no changes vs `%s`).",
	MsgDiffNoDiff2HTML:               "`diff2html` is not installed, sending plain text diff.\nInstall: `npm install -g diff2html-cli`",
	MsgDirChanged:                    "Work directory changed to: `%s`\nThe next session will start in this directory.",
	MsgDirCurrent:                    "Current work directory: `%s`",
	MsgDirReset:                      "Work directory reset to the configured default: `%s`",
	MsgDirUsage:                      "Usage: `/dir <path>`\n       `/dir reset`\nExample: `/dir ../project`",
	MsgDirNotSupported:               "This agent does not support dynamic work directory switching.",
	MsgDirInvalidPath:                "Directory does not exist: `%s`",
	MsgDirHistoryTitle:               "History:",
	MsgDirHistoryHint:                "Use `/dir <number>` to switch, or `/dir -` for previous.",
	MsgDirInvalidIndex:               "Invalid history index: %d",
	MsgDirNoHistory:                  "No directory history available.",
	MsgDirNoPrevious:                 "No previous directory in history.",
	MsgDirCardTitle:                  "Working directory",
	MsgDirCardPageHint:               "Page %d/%d — use `/dir <page>` or the buttons below.",
	MsgDirCardEmptyHistory:           "No directory history yet. Type `/dir <path>` to switch, or use **Reset** to restore the default.",
	MsgDirCardReset:                  "Reset",
	MsgDirCardPrev:                   "Previous",
	MsgShowUsage:                     "Usage: `/show <path|path:line|path:start-end|dir/>`\nExample: `/show svc/recovery_session_reconciler.go:12`",
	MsgShowParseError:                "Cannot parse reference: `%s`",
	MsgShowNotFound:                  "Referenced path does not exist: `%s`",
	MsgShowDirWithLocation:           "Directory references cannot include line information: `%s`",
	MsgShowReadFailed:                "Failed to read reference: %s",
}

func (i *I18n) T(key MsgKey) string {
	if msg, ok := messages[key]; ok {
		return msg
	}
	return string(key)
}

func (i *I18n) Tf(key MsgKey, args ...interface{}) string {
	template := i.T(key)
	return fmt.Sprintf(template, args...)
}
