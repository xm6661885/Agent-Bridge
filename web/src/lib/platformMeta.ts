export interface FieldDef {
  key: string;
  label: string;
  required?: boolean;
  type?: 'text' | 'password' | 'number' | 'boolean' | 'select';
  placeholder?: string;
  hint?: string;
  group?: 'basic' | 'advanced';
  options?: string[];
  showWhen?: Record<string, string[]>;
}

export interface PlatformMeta {
  label: string;
  fields: FieldDef[];
}

export const platformMeta: Record<string, PlatformMeta> = {
  telegram: {
    label: 'Telegram',
    fields: [
      { key: 'token', label: "Bot Token", required: true, type: 'password', placeholder: '123456:ABC-DEF...' },
      { key: 'allow_from', label: "Allowed users", placeholder: '* (all)', group: 'advanced', hint: "Telegram user IDs, comma-separated" },
      { key: 'group_reply_all', label: "Reply to all group messages", type: 'boolean', group: 'advanced' },
      { key: 'share_session_in_channel', label: "Shared group session", type: 'boolean', group: 'advanced' },
    ],
  },
  qq: {
    label: 'QQ (OneBot v11)',
    fields: [
      { key: 'ws_url', label: "WebSocket URL", required: true, placeholder: 'ws://127.0.0.1:3001' },
      { key: 'token', label: "Access Token", type: 'password', group: 'advanced' },
      { key: 'allow_from', label: "Allowed users", placeholder: '* (all)', group: 'advanced' },
      { key: 'share_session_in_channel', label: "Shared group session", type: 'boolean', group: 'advanced' },
    ],
  },
};
