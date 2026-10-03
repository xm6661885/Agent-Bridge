import api from './client';

export interface WeixinBeginResponse {
  qr_key: string;
  qr_url: string;
}

export interface WeixinPollResponse {
  status: 'wait' | 'scaned' | 'confirmed' | 'expired';
  bot_token?: string;
  ilink_bot_id?: string;
  base_url?: string;
  ilink_user_id?: string;
}

export const setupWeixinBegin = (apiUrl?: string) =>
  api.post<WeixinBeginResponse>('/setup/weixin/begin', { api_url: apiUrl });

export const setupWeixinPoll = (qrKey: string, apiUrl?: string) =>
  api.post<WeixinPollResponse>('/setup/weixin/poll', { qr_key: qrKey, api_url: apiUrl });

export const setupWeixinSave = (body: {
  project: string; token: string; base_url?: string;
  ilink_bot_id?: string; ilink_user_id?: string; work_dir?: string; agent_type?: string;
}) => api.post<{ message: string; restart_required: boolean }>('/setup/weixin/save', body);
