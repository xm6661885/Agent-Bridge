import { useEffect, useState, useCallback } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import {
  ArrowLeft, Plug, Settings, Layers,
  Trash2, Plus,
} from 'lucide-react';
import { Card, Badge, Button, Input, Modal } from '@/components/ui';
import { getProject, updateProject, deleteProject, listAgentTypes, type ProjectDetail as ProjectDetailType } from '@/api/projects';
import { restartSystem } from '@/api/status';
import { cn } from '@/lib/utils';
import PlatformSetupQR from './PlatformSetupQR';
import PlatformManualForm from './PlatformManualForm';
import { platformMeta } from '@/lib/platformMeta';

const PLATFORM_OPTIONS: { key: string; label: string; color: string; abbr: string; qr?: boolean }[] = [
  { key: 'weixin', label: 'WeChat', abbr: 'WX', color: 'bg-green-50 dark:bg-green-900/30 text-green-600 dark:text-green-400', qr: true },
  { key: 'telegram', label: 'Telegram', abbr: 'TG', color: 'bg-sky-50 dark:bg-sky-900/30 text-sky-600 dark:text-sky-400' },
  { key: 'qq', label: 'QQ (OneBot)', abbr: 'QQ', color: 'bg-cyan-50 dark:bg-cyan-900/30 text-cyan-600 dark:text-cyan-400' },
];

// Permission mode options per agent type. The values must match the keys
// emitted by each agent's `normalizeMode` / `PermissionModes` so that
// "save" round-trips correctly. See:
//   claudecode: agent/claudecode/claudecode.go:818  (PermissionModes)
//   codex:      agent/codex/codex.go:129            (normalizeMode)
const CLAUDECODE_MODE_OPTIONS: { value: string; label: string }[] = [
  { value: 'default', label: 'default' },
  { value: 'acceptEdits', label: 'acceptEdits (edit)' },
  { value: 'plan', label: 'plan' },
  { value: 'bypassPermissions', label: 'bypassPermissions (yolo)' },
  { value: 'dontAsk', label: 'dontAsk' },
];
const CODEX_MODE_OPTIONS: { value: string; label: string }[] = [
  { value: 'suggest', label: 'suggest (default)' },
  { value: 'auto-edit', label: 'auto-edit' },
  { value: 'full-auto', label: 'full-auto' },
  { value: 'yolo', label: 'yolo (bypass)' },
];
const MODE_OPTIONS_BY_AGENT: Record<string, { value: string; label: string }[]> = {
  claudecode: CLAUDECODE_MODE_OPTIONS,
  codex: CODEX_MODE_OPTIONS,
};

const isQRPlatform = (type: string) => type === 'weixin';

type Tab = 'overview' | 'settings';

export default function ProjectDetail() {
  const { name } = useParams<{ name: string }>();
  const [tab, setTab] = useState<Tab>('overview');
  const [project, setProject] = useState<ProjectDetailType | null>(null);
  const [loading, setLoading] = useState(true);

  // Settings form
  const [adminFrom, setAdminFrom] = useState('');
  const [disabledCmds, setDisabledCmds] = useState('');
  const [workDir, setWorkDir] = useState('');
  const [agentMode, setAgentMode] = useState('');
  const [showCtxIndicator, setShowCtxIndicator] = useState(true);
  const [showWorkdirIndicator, setShowWorkdirIndicator] = useState(true);
  const [replyFooter, setReplyFooter] = useState(true);
  const [injectSender, setInjectSender] = useState(false);
  const [platformAllowFrom, setPlatformAllowFrom] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [cleanupProgress, setCleanupProgress] = useState(false);
  const [collapseTools, setCollapseTools] = useState(false);

  // Agent type
  const [agentTypes, setAgentTypes] = useState<string[]>([]);
  const [selectedAgentType, setSelectedAgentType] = useState('');

  // Add platform
  const [showAddPlatform, setShowAddPlatform] = useState(false);
  const [addPlatType, setAddPlatType] = useState('');
  const [showRestartModal, setShowRestartModal] = useState(false);

  // Delete project
  const navigate = useNavigate();
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [deleting, setDeleting] = useState(false);

  // Permission mode options track the *effective* agent type: a freshly-picked
  // type overrides the saved one so the dropdown matches what would be saved.
  // Unknown agent types fall back to ClaudeCode (matches the previous hardcoded
  // behavior) so this change is non-breaking for other agents.
  const effectiveAgentType = selectedAgentType || project?.agent_type || '';
  const modeOptions = MODE_OPTIONS_BY_AGENT[effectiveAgentType] || CLAUDECODE_MODE_OPTIONS;

  const handleDeleteProject = async () => {
    if (!name) return;
    setDeleting(true);
    try {
      const res = await deleteProject(name);
      setShowDeleteConfirm(false);
      if (res.restart_required && window.confirm("Project removed. Restart service to take effect?")) {
        await restartSystem();
        // Wait for service to come back up before navigating
        await waitForService(8000);
      }
      navigate('/projects');
    } catch (e: any) {
      alert(e?.message || String(e));
    } finally {
      setDeleting(false);
    }
  };

  const waitForService = (maxMs: number) =>
    new Promise<void>((resolve) => {
      const start = Date.now();
      const poll = () => {
        fetch('/api/v1/status')
          .then((r) => { if (r.ok) resolve(); else throw new Error(); })
          .catch(() => {
            if (Date.now() - start > maxMs) { resolve(); return; }
            setTimeout(poll, 500);
          });
      };
      setTimeout(poll, 1500);
    });

  const fetchAll = useCallback(async () => {
    if (!name) return;
    try {
      setLoading(true);
      const [proj, at] = await Promise.allSettled([
        getProject(name),
        listAgentTypes(),
      ]);
      if (proj.status === 'fulfilled') {
        setProject(proj.value);
        setAdminFrom(proj.value.settings?.admin_from || '');
        setDisabledCmds(proj.value.settings?.disabled_commands?.join(', ') || '');
        setWorkDir(proj.value.work_dir || '');
        setAgentMode(proj.value.agent_mode || 'default');
        setSelectedAgentType(proj.value.agent_type || '');
        // Prefer the engine's resolved values (project > global > default);
        // top-level fields are only explicit project overrides.
        const eff = proj.value.display_effective;
        setCleanupProgress(eff?.cleanup_progress_on_complete ?? false);
        setCollapseTools(eff?.collapse_tool_messages ?? false);
        setShowCtxIndicator(eff?.show_context_indicator ?? proj.value.show_context_indicator !== false);
        setShowWorkdirIndicator(eff?.show_workdir_indicator ?? proj.value.show_workdir_indicator !== false);
        setReplyFooter(eff?.reply_footer ?? proj.value.reply_footer !== false);
        setInjectSender(proj.value.inject_sender === true);
        const afMap: Record<string, string> = {};
        proj.value.platform_configs?.forEach(pc => {
          if (pc.allow_from !== undefined) afMap[pc.type] = pc.allow_from;
        });
        setPlatformAllowFrom(afMap);
      }
      if (at.status === 'fulfilled') {
        setAgentTypes((at.value.agents || []).sort());
      }
    } finally {
      setLoading(false);
    }
  }, [name]);

  useEffect(() => {
    fetchAll();
    const handler = () => fetchAll();
    window.addEventListener('agent-bridge:refresh', handler);
    return () => window.removeEventListener('agent-bridge:refresh', handler);
  }, [fetchAll]);

  const handleSaveSettings = async () => {
    if (!name) return;
    setSaving(true);
    try {
      const agentTypeChanged = project && selectedAgentType !== project.agent_type;
      const eff = project?.display_effective;
      const effCtx = eff?.show_context_indicator ?? project?.show_context_indicator !== false;
      const effWorkdir = eff?.show_workdir_indicator ?? project?.show_workdir_indicator !== false;
      const effFooter = eff?.reply_footer ?? project?.reply_footer !== false;
      const res = await updateProject(name, {
        admin_from: adminFrom,
        disabled_commands: disabledCmds.split(',').map(s => s.trim()).filter(Boolean),
        work_dir: workDir,
        mode: agentMode,
        ...(agentTypeChanged ? { agent_type: selectedAgentType } : {}),
        // Only persist toggles the user actually changed, so unchanged ones keep
        // inheriting the global setting instead of being pinned per-project.
        ...(showCtxIndicator !== effCtx ? { show_context_indicator: showCtxIndicator } : {}),
        ...(showWorkdirIndicator !== effWorkdir ? { show_workdir_indicator: showWorkdirIndicator } : {}),
        ...(replyFooter !== effFooter ? { reply_footer: replyFooter } : {}),
        ...(cleanupProgress !== (eff?.cleanup_progress_on_complete ?? false) ? { cleanup_progress_on_complete: cleanupProgress } : {}),
        ...(collapseTools !== (eff?.collapse_tool_messages ?? false) ? { collapse_tool_messages: collapseTools } : {}),
        inject_sender: injectSender,
        platform_allow_from: platformAllowFrom,
      });
      if (res && (res as any).restart_required) {
        setShowRestartModal(true);
        return;
      }
      await fetchAll();
    } finally {
      setSaving(false);
    }
  };

  const tabs: { key: Tab; label: string; icon: React.ElementType }[] = [
    { key: 'overview', label: 'Overview', icon: Layers },
    { key: 'settings', label: 'Settings', icon: Settings },
  ];

  if (loading && !project) {
    return <div className="flex items-center justify-center h-64 text-gray-400 animate-pulse">Loading...</div>;
  }

  return (
    <div className="space-y-6 animate-fade-in ">
      {/* Back + title */}
      <div className="flex items-center gap-3">
        <Link to="/projects" className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors">
          <ArrowLeft size={18} className="text-gray-400" />
        </Link>
        <h2 className="text-xl font-bold text-gray-900 dark:text-white">{name}</h2>
        {project && <Badge variant="info">{project.agent_type}</Badge>}
      </div>

      {/* Tabs */}
      <div className="flex gap-2">
        {tabs.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={cn(
              'flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all',
              tab === key
                ? 'bg-gray-900 dark:bg-gray-700 text-white shadow-md'
                : 'bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-700'
            )}
          >
            <Icon size={16} />
            {label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === 'overview' && project && (
        <div className="space-y-4">
          <Card>
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-semibold text-gray-900 dark:text-white">{"Platforms"}</h3>
              <Button size="sm" onClick={() => { setShowAddPlatform(true); setAddPlatType(''); }}>
                <Plus size={14} /> {"Add platform"}
              </Button>
            </div>
            <div className="flex flex-wrap gap-2">
              {project.platforms?.map((p) => (
                <Badge key={p.type} variant={p.connected ? 'success' : 'danger'}>
                  <Plug size={12} className="mr-1" /> {p.type} {p.connected ? 'Connected' : 'Disconnected'}
                </Badge>
              ))}
            </div>
          </Card>
          <Card>
            <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-3">{"Sessions"}</h3>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              {project.sessions_count} {"Sessions".toLowerCase()}
            </p>
            {project.active_session_keys?.length > 0 && (
              <div className="mt-2 flex flex-wrap gap-1">
                {project.active_session_keys.map((k) => (
                  <Badge key={k} variant="default">{k}</Badge>
                ))}
              </div>
            )}
          </Card>
        </div>
      )}

      {tab === 'settings' && project && (
        <div className="space-y-4">
        {/* Agent settings */}
        <Card>
          <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-4">{"Agent"}</h3>
          <div className="space-y-4 max-w-lg">
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                {"Agent type"}
              </label>
              <select
                value={selectedAgentType}
                onChange={(e) => setSelectedAgentType(e.target.value)}
                className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-accent/50"
              >
                {agentTypes.map(a => <option key={a} value={a}>{a}</option>)}
                {selectedAgentType && !agentTypes.includes(selectedAgentType) && (
                  <option value={selectedAgentType}>{selectedAgentType}</option>
                )}
              </select>
              {selectedAgentType !== project.agent_type && (
                <p className="text-[11px] text-amber-500 mt-1">{"Changing agent type requires restart."}</p>
              )}
            </div>
            <Input label={"Working directory"} value={workDir} onChange={(e) => setWorkDir(e.target.value)} placeholder="/path/to/project" />
            <div>
              <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                {"Permission mode"}
              </label>
              <select
                value={agentMode}
                onChange={(e) => setAgentMode(e.target.value)}
                className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-accent/50"
              >
                {modeOptions.map((opt) => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
                {agentMode && !modeOptions.some((o) => o.value === agentMode) && (
                  <option value={agentMode}>{agentMode}</option>
                )}
              </select>
            </div>
          </div>
        </Card>

        {/* General settings */}
        <Card>
          <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-4">{"General"}</h3>
          <div className="space-y-4 max-w-lg">
            <label className="flex items-center justify-between text-sm">
              Clean up progress after completion
              <input type="checkbox" checked={cleanupProgress} onChange={e => setCleanupProgress(e.target.checked)} />
            </label>
            <label className="flex items-center justify-between text-sm">
              Collapse tool messages
              <input type="checkbox" checked={collapseTools} onChange={e => setCollapseTools(e.target.checked)} />
            </label>
            <div className="flex items-center justify-between">
              <div>
                <label className="text-sm font-medium text-gray-700 dark:text-gray-300">{"Reply footer"}</label>
                <p className="text-[11px] text-gray-400 mt-0.5">{"Append model/usage metadata to replies"}</p>
              </div>
              <button
                onClick={() => setReplyFooter(!replyFooter)}
                className={cn('w-10 h-6 rounded-full transition-colors', replyFooter ? 'bg-accent' : 'bg-gray-300 dark:bg-gray-700')}
              >
                <div className={cn('w-4 h-4 bg-white rounded-full transition-transform mx-1', replyFooter ? 'translate-x-4' : 'translate-x-0')} />
              </button>
            </div>
            <div className="flex items-center justify-between">
              <div>
                <label className="text-sm font-medium text-gray-700 dark:text-gray-300">{"Context indicator"}</label>
                <p className="text-[11px] text-gray-400 mt-0.5">{"Show [ctx: ~N%] suffix on replies"}</p>
              </div>
              <button
                onClick={() => setShowCtxIndicator(!showCtxIndicator)}
                className={cn('w-10 h-6 rounded-full transition-colors', showCtxIndicator ? 'bg-accent' : 'bg-gray-300 dark:bg-gray-700')}
              >
                <div className={cn('w-4 h-4 bg-white rounded-full transition-transform mx-1', showCtxIndicator ? 'translate-x-4' : 'translate-x-0')} />
              </button>
            </div>
            <div className="flex items-center justify-between">
              <div>
                <label className="text-sm font-medium text-gray-700 dark:text-gray-300">{"Footer line 2: workdir"}</label>
                <p className="text-[11px] text-gray-400 mt-0.5">{"Show workspace directory line in the reply footer"}</p>
              </div>
              <button
                onClick={() => setShowWorkdirIndicator(!showWorkdirIndicator)}
                className={cn('w-10 h-6 rounded-full transition-colors', showWorkdirIndicator ? 'bg-accent' : 'bg-gray-300 dark:bg-gray-700')}
              >
                <div className={cn('w-4 h-4 bg-white rounded-full transition-transform mx-1', showWorkdirIndicator ? 'translate-x-4' : 'translate-x-0')} />
              </button>
            </div>
            <div className="flex items-center justify-between">
              <div>
                <label className="text-sm font-medium text-gray-700 dark:text-gray-300">{"Inject sender"}</label>
                <p className="text-[11px] text-gray-400 mt-0.5">{"Prepend sender identity to messages sent to agent"}</p>
              </div>
              <button
                onClick={() => setInjectSender(!injectSender)}
                className={cn('w-10 h-6 rounded-full transition-colors', injectSender ? 'bg-accent' : 'bg-gray-300 dark:bg-gray-700')}
              >
                <div className={cn('w-4 h-4 bg-white rounded-full transition-transform mx-1', injectSender ? 'translate-x-4' : 'translate-x-0')} />
              </button>
            </div>
            <Input label={"Admin from"} value={adminFrom} onChange={(e) => setAdminFrom(e.target.value)} placeholder="user1,user2 or *" />
            <Input label={"Disabled commands"} value={disabledCmds} onChange={(e) => setDisabledCmds(e.target.value)} placeholder="restart, shell" />
          </div>
        </Card>

        {/* Per-platform allow_from */}
        {project.platform_configs && project.platform_configs.length > 0 && (
        <Card>
          <h3 className="text-sm font-semibold text-gray-900 dark:text-white mb-4">{"Platform access control"}</h3>
          <div className="space-y-3 max-w-lg">
            {project.platform_configs.map(pc => (
              <Input
                key={pc.type}
                label={`${pc.type} — ${"Allowed users"}`}
                value={platformAllowFrom[pc.type] ?? pc.allow_from ?? ''}
                onChange={(e) => setPlatformAllowFrom(prev => ({ ...prev, [pc.type]: e.target.value }))}
                placeholder='user1,user2 or *'
              />
            ))}
          </div>
        </Card>
        )}

        <div className="max-w-lg">
          <Button loading={saving} onClick={handleSaveSettings}>{"Save"}</Button>
        </div>
        <Card>
          <h3 className="text-sm font-semibold text-red-600 dark:text-red-400 mb-3">{"Danger Zone"}</h3>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-700 dark:text-gray-300">{"Delete Project"}</p>
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{"Remove this project from config. Requires restart."}</p>
            </div>
            <Button variant="danger" size="sm" onClick={() => setShowDeleteConfirm(true)}>
              <Trash2 size={14} /> {"Delete"}
            </Button>
          </div>
        </Card>
        </div>
      )}

      {/* Delete confirmation */}
      <Modal open={showDeleteConfirm} onClose={() => setShowDeleteConfirm(false)} title={"Delete Project"}>
        <div className="space-y-4 py-2">
          <p className="text-sm text-gray-600 dark:text-gray-400">
            {`Are you sure you want to delete project "${name}"? This will remove it from the config file.`}
          </p>
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setShowDeleteConfirm(false)}>{"Cancel"}</Button>
            <Button variant="danger" onClick={handleDeleteProject} disabled={deleting}>
              {deleting ? "Deleting..." : "Delete"}
            </Button>
          </div>
        </div>
      </Modal>

      {/* Add Platform Modal */}
      <Modal open={showAddPlatform} onClose={() => setShowAddPlatform(false)} title={"Add platform"}>
        {!addPlatType ? (
          <div className="space-y-3 py-2">
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-2">
              {"Choose a platform to connect:"}
            </p>
            <div className="grid grid-cols-2 gap-2 max-h-80 overflow-y-auto">
              {PLATFORM_OPTIONS.map(({ key, label, color, qr, abbr }) => (
                <button
                  key={key}
                  onClick={() => setAddPlatType(key)}
                  className="flex items-center gap-2.5 p-3 rounded-xl border border-gray-200 dark:border-gray-700 hover:border-accent/50 hover:bg-accent/5 transition-all text-left"
                >
                  <div className={`w-9 h-9 rounded-lg ${color} flex items-center justify-center shrink-0 font-bold text-xs`}>
                    {abbr}
                  </div>
                  <div className="min-w-0">
                    <div className="text-sm font-medium text-gray-900 dark:text-white truncate">{label}</div>
                    <div className="text-[11px] text-gray-400">
                      {qr ? "Scan QR code to connect" : "Manual setup"}
                    </div>
                  </div>
                </button>
              ))}
            </div>
          </div>
        ) : isQRPlatform(addPlatType) ? (
          <PlatformSetupQR
            platformType={addPlatType as 'weixin'}
            projectName={name!}
            onComplete={() => {
              setShowAddPlatform(false);
              setShowRestartModal(true);
            }}
            onCancel={() => setAddPlatType('')}
          />
        ) : platformMeta[addPlatType] ? (
          <PlatformManualForm
            platformType={addPlatType}
            projectName={name!}
            onComplete={() => {
              setShowAddPlatform(false);
              setShowRestartModal(true);
            }}
            onCancel={() => setAddPlatType('')}
          />
        ) : (
          <div className="space-y-4 py-4 text-center">
            <p className="text-sm text-gray-600 dark:text-gray-400">
              {`For ${PLATFORM_OPTIONS.find(o => o.key === addPlatType)?.label || addPlatType}, please configure credentials in config.toml and restart the service.`}
            </p>
            <Button variant="secondary" onClick={() => setAddPlatType('')}>{"Back"}</Button>
          </div>
        )}
      </Modal>

      {/* Restart Required Modal */}
      <Modal open={showRestartModal} onClose={() => setShowRestartModal(false)} title={"Restart required"}>
        <div className="space-y-4 py-2">
          <p className="text-sm text-gray-600 dark:text-gray-400">
            {"Restart the service for the new platform to take effect."}
          </p>
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => { setShowRestartModal(false); setTimeout(fetchAll, 300); }}>
              {"Later"}
            </Button>
            <Button onClick={async () => { await restartSystem(); setShowRestartModal(false); await waitForService(8000); await fetchAll(); }}>
              {"Restart now"}
            </Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
