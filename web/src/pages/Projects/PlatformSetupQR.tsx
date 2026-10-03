import { useState, useEffect, useRef, useCallback } from 'react';
import { QRCodeSVG } from 'qrcode.react';
import { Loader2, CheckCircle2, XCircle, RefreshCw, Smartphone, RotateCcw } from 'lucide-react';
import { Button } from '@/components/ui';
import {
  setupWeixinBegin, setupWeixinPoll, setupWeixinSave,
} from '@/api/setup';
import { restartSystem } from '@/api/status';

type PlatformKind = 'weixin';
type Phase = 'idle' | 'loading' | 'scanning' | 'scanned' | 'completed' | 'expired' | 'error' | 'saving';

interface Props {
  platformType: PlatformKind;
  projectName: string;
  workDir?: string;
  agentType?: string;
  onComplete: () => void;
  onCancel: () => void;
}

export default function PlatformSetupQR({ platformType, projectName, workDir, agentType, onComplete, onCancel }: Props) {
  const [phase, setPhase] = useState<Phase>('idle');
  const [qrUrl, setQrUrl] = useState('');
  const [error, setError] = useState('');
  const cancelledRef = useRef(false);
  const pollingRef = useRef(false);

  // Weixin state
  const weixinRef = useRef({ qrKey: '' });

  useEffect(() => {
    return () => { cancelledRef.current = true; };
  }, []);

  const startWeixinFlow = useCallback(async () => {
    setPhase('loading');
    setError('');
    cancelledRef.current = false;
    pollingRef.current = false;
    try {
      const res = await setupWeixinBegin();
      console.log('[weixin-setup] begin response:', { qr_key: res.qr_key, qr_url_len: res.qr_url?.length, qr_url_prefix: res.qr_url?.slice(0, 80) });
      const qrKey = res.qr_key;
      weixinRef.current.qrKey = qrKey;
      setQrUrl(res.qr_url);
      setPhase('scanning');

      console.log('[weixin-setup] starting poll loop, qrKey=', qrKey, 'cancelledRef=', cancelledRef.current);
      let consecutiveErrors = 0;
      while (!cancelledRef.current) {
        try {
          console.log('[weixin-setup] sending poll request, qrKey=', qrKey);
          const pollRes = await setupWeixinPoll(qrKey);
          console.log('[weixin-setup] poll response:', pollRes);
          consecutiveErrors = 0;
          if (cancelledRef.current) break;

          switch (pollRes.status) {
            case 'scaned':
              setPhase('scanned');
              break;
            case 'confirmed':
              setPhase('saving');
              await setupWeixinSave({
                project: projectName,
                token: pollRes.bot_token!,
                base_url: pollRes.base_url,
                ilink_bot_id: pollRes.ilink_bot_id,
                ilink_user_id: pollRes.ilink_user_id,
                work_dir: workDir,
                agent_type: agentType,
              });
              setPhase('completed');
              return;
            case 'expired':
              setPhase('expired');
              return;
          }
        } catch (e: any) {
          console.error('[weixin-setup] poll error:', e);
          if (cancelledRef.current) break;
          consecutiveErrors++;
          if (consecutiveErrors >= 5) {
            setError(e?.message || String(e));
            setPhase('error');
            return;
          }
        }
        await sleep(500);
      }
    } catch (e: any) {
      console.error('[weixin-setup] begin error:', e);
      setError(e?.message || String(e));
      setPhase('error');
    }
  }, [projectName]);

  const startFlow = startWeixinFlow;

  const handleRetry = () => {
    cancelledRef.current = false;
    pollingRef.current = false;
    startFlow();
  };

  const platformLabel = "WeChat (ilink)";

  const scanHint = "Open WeChat and scan the QR code";

  return (
    <div className="flex flex-col items-center gap-4 py-4">
      {phase === 'idle' && (
        <>
          <Smartphone size={48} className="text-gray-400" />
          <p className="text-sm text-gray-600 dark:text-gray-400 text-center">
            {`Scan a QR code with your phone to quickly connect ${platformLabel}.`}
          </p>
          <Button onClick={startFlow}>
            {"Start QR Setup"}
          </Button>
        </>
      )}

      {phase === 'loading' && (
        <div className="flex flex-col items-center gap-3 py-8">
          <Loader2 size={32} className="animate-spin text-accent" />
          <p className="text-sm text-gray-500">{"Generating QR code..."}</p>
        </div>
      )}

      {(phase === 'scanning' || phase === 'scanned' || phase === 'saving') && (
        <>
          <div className="p-4 bg-white rounded-xl shadow-sm border border-gray-200">
            <QRCodeSVG value={qrUrl} size={200} level="M" />
          </div>
          <p className="text-sm text-gray-600 dark:text-gray-400 text-center max-w-xs">
            {phase === 'scanned'
              ? "Scanned! Please confirm on your phone..."
              : phase === 'saving'
                ? "Saving configuration..."
                : scanHint}
          </p>
          {phase === 'scanning' && (
            <div className="flex items-center gap-2 text-xs text-gray-400">
              <Loader2 size={12} className="animate-spin" />
              {"Waiting for scan..."}
            </div>
          )}
          {phase === 'scanned' && (
            <div className="flex items-center gap-2 text-xs text-accent">
              <Loader2 size={12} className="animate-spin" />
              {"Waiting for confirmation..."}
            </div>
          )}
          {phase === 'saving' && (
            <div className="flex items-center gap-2 text-xs text-accent">
              <Loader2 size={12} className="animate-spin" />
              {"Saving configuration..."}
            </div>
          )}
        </>
      )}

      {phase === 'completed' && (
        <div className="flex flex-col items-center gap-3 py-4">
          <CheckCircle2 size={48} className="text-green-500" />
          <p className="text-sm font-medium text-green-700 dark:text-green-400">
            {"Platform connected successfully!"}
          </p>
          <p className="text-xs text-gray-500 text-center">
            {"Restart the service for the new platform to take effect."}
          </p>
          <div className="flex gap-2">
            <Button
              variant="secondary"
              onClick={async () => {
                try {
                  await restartSystem();
                  setPhase('restarting' as Phase);
                  setTimeout(() => onComplete(), 3000);
                } catch (e: any) {
                  setError(e?.message || String(e));
                }
              }}
            >
              <RotateCcw size={14} /> {"Restart now"}
            </Button>
            <Button onClick={onComplete}>{"Later"}</Button>
          </div>
        </div>
      )}

      {phase === ('restarting' as Phase) && (
        <div className="flex flex-col items-center gap-3 py-4">
          <Loader2 size={32} className="animate-spin text-accent" />
          <p className="text-sm text-gray-600 dark:text-gray-400">
            {"Restarting service..."}
          </p>
        </div>
      )}

      {phase === 'expired' && (
        <div className="flex flex-col items-center gap-3 py-4">
          <XCircle size={48} className="text-amber-500" />
          <p className="text-sm text-amber-700 dark:text-amber-400">
            {"QR code expired."}
          </p>
          <Button onClick={handleRetry}>
            <RefreshCw size={14} /> {"Retry"}
          </Button>
        </div>
      )}

      {phase === 'error' && (
        <div className="flex flex-col items-center gap-3 py-4">
          <XCircle size={48} className="text-red-500" />
          <p className="text-sm text-red-700 dark:text-red-400">{error}</p>
          <Button onClick={handleRetry}>
            <RefreshCw size={14} /> {"Retry"}
          </Button>
        </div>
      )}

      {phase !== 'completed' && (
        <button
          onClick={onCancel}
          className="text-xs text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 mt-2"
        >
          {"Cancel"}
        </button>
      )}
    </div>
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
