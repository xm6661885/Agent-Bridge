import { useState, useRef, useEffect, useMemo } from 'react';
import {
  Slash, Search, MessageSquarePlus, List, ArrowRightLeft, Eye, History,
  Square, Brain, Cpu, Layers,
  Terminal, Tag, Wrench, Trash2,
  FolderOpen, HelpCircle, User,
} from 'lucide-react';
import { cn } from '@/lib/utils';

export interface SlashCommand {
  cmd: string;
  label: string;
  icon: React.ElementType;
  group: 'session' | 'settings' | 'info' | 'advanced';
  local?: boolean; // handled locally, not sent to bridge
}

export const slashCommands: SlashCommand[] = [
  // Session
  { cmd: '/new', label: "New session", icon: MessageSquarePlus, group: 'session' },
  { cmd: '/list', label: "Session list", icon: List, group: 'session' },
  { cmd: '/switch', label: "Switch session", icon: ArrowRightLeft, group: 'session' },
  { cmd: '/current', label: "Current session", icon: Eye, group: 'session' },
  { cmd: '/history', label: "History", icon: History, group: 'session' },
  { cmd: '/stop', label: "Stop session", icon: Square, group: 'session' },
  // Settings
  { cmd: '/model', label: "Model", icon: Brain, group: 'settings' },
  { cmd: '/effort', label: "Reasoning", icon: Cpu, group: 'settings' },
  { cmd: '/mode', label: "Mode", icon: Layers, group: 'settings' },
  // Info
  { cmd: '/help', label: "Help", icon: HelpCircle, group: 'info' },
  { cmd: '/whoami', label: "Who am I", icon: User, group: 'info' },
  { cmd: '/commands', label: "All commands", icon: Terminal, group: 'info' },
  // Advanced
  { cmd: '/dir', label: "Work directory", icon: FolderOpen, group: 'advanced' },
  { cmd: '/alias', label: "Aliases", icon: Tag, group: 'advanced' },
  { cmd: '/delete-mode', label: "Delete mode", icon: Trash2, group: 'advanced' },
];

const groupOrder: { key: string; label: string }[] = [
  { key: 'session', label: "Session" },
  { key: 'settings', label: "Settings" },
  { key: 'info', label: "Info" },
  { key: 'advanced', label: "Advanced" },
];

interface Props {
  open: boolean;
  onClose: () => void;
  onSelect: (cmd: SlashCommand) => void;
  anchorRef: React.RefObject<HTMLElement | null>;
}

export default function CommandPalette({ open, onClose, onSelect, anchorRef }: Props) {
  const [query, setQuery] = useState('');
  const [activeIdx, setActiveIdx] = useState(0);
  const panelRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);

  const filtered = useMemo(() => {
    if (!query) return slashCommands;
    const q = query.toLowerCase().replace(/^\//, '');
    return slashCommands.filter(
      (c) => c.cmd.toLowerCase().includes(q) || c.label.toLowerCase().includes(q),
    );
  }, [query]);

  useEffect(() => {
    if (open) {
      setQuery('');
      setActiveIdx(0);
      setTimeout(() => searchRef.current?.focus(), 50);
    }
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const handleClick = (e: MouseEvent) => {
      if (panelRef.current && !panelRef.current.contains(e.target as Node) &&
          anchorRef.current && !anchorRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [open, onClose, anchorRef]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActiveIdx((i) => Math.min(i + 1, filtered.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActiveIdx((i) => Math.max(i - 1, 0));
    } else if (e.key === 'Enter' && filtered[activeIdx]) {
      e.preventDefault();
      onSelect(filtered[activeIdx]);
    } else if (e.key === 'Escape') {
      onClose();
    }
  };

  if (!open) return null;

  const grouped = groupOrder
    .map((g) => ({
      ...g,
      items: filtered.filter((c) => c.group === g.key),
    }))
    .filter((g) => g.items.length > 0);

  let flatIdx = 0;

  return (
    <div
      ref={panelRef}
      className={cn(
        'absolute bottom-full left-0 mb-2 w-80 max-h-[420px] flex flex-col rounded-xl overflow-hidden z-50',
        'bg-white/95 backdrop-blur-xl border border-gray-200/80 shadow-2xl shadow-black/15',
        'dark:bg-[rgba(20,20,20,0.96)] dark:border-white/[0.1] dark:shadow-black/50',
        'animate-in slide-in-from-bottom-2 fade-in duration-200',
      )}
    >
      <div className="px-3 pt-3 pb-2">
        <div className="flex items-center gap-2 px-2.5 py-2 rounded-lg bg-gray-100/80 dark:bg-white/[0.06] border border-gray-200/50 dark:border-white/[0.05]">
          <Search size={14} className="text-gray-400 shrink-0" />
          <input
            ref={searchRef}
            value={query}
            onChange={(e) => { setQuery(e.target.value); setActiveIdx(0); }}
            onKeyDown={handleKeyDown}
            placeholder={"Search commands..."}
            className="flex-1 bg-transparent text-sm text-gray-900 dark:text-white placeholder:text-gray-400 outline-none"
          />
        </div>
      </div>
      <div className="flex-1 overflow-y-auto px-1.5 pb-2">
        {grouped.map((g) => (
          <div key={g.key} className="mb-1">
            <div className="px-2.5 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-gray-400 dark:text-gray-500">
              {g.label}
            </div>
            {g.items.map((cmd) => {
              const idx = flatIdx++;
              const Icon = cmd.icon;
              return (
                <button
                  key={cmd.cmd}
                  type="button"
                  onClick={() => onSelect(cmd)}
                  onMouseEnter={() => setActiveIdx(idx)}
                  className={cn(
                    'w-full flex items-center gap-2.5 px-2.5 py-2 rounded-lg text-sm transition-colors',
                    idx === activeIdx
                      ? 'bg-accent/15 text-gray-900 dark:text-white'
                      : 'text-gray-700 dark:text-gray-300 hover:bg-gray-100/80 dark:hover:bg-white/[0.06]',
                  )}
                >
                  <Icon size={15} className={idx === activeIdx ? 'text-accent' : 'text-gray-400'} />
                  <span className="font-mono text-xs text-gray-500 dark:text-gray-400 w-24 text-left">{cmd.cmd}</span>
                  <span className="flex-1 text-left truncate">{cmd.label}</span>
                </button>
              );
            })}
          </div>
        ))}
        {filtered.length === 0 && (
          <div className="text-center text-sm text-gray-400 py-6">{"No data"}</div>
        )}
      </div>
    </div>
  );
}
