import { NavLink } from 'react-router-dom';
import {
  LayoutDashboard,
  FolderKanban,
  MessageSquare,
  Settings,
  ChevronLeft,
  ChevronRight,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { useState } from 'react';

const navItems = [
  { key: 'dashboard', label: "Dashboard", path: '/', icon: LayoutDashboard },
  { key: 'projects', label: "Projects", path: '/projects', icon: FolderKanban },
  { key: 'chat', label: "Chat", path: '/chat', icon: MessageSquare },
  { key: 'system', label: "System", path: '/system', icon: Settings },
];

export default function Sidebar() {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside
      className={cn(
        'h-screen flex flex-col border-r transition-all duration-300 ease-out',
        'bg-white/75 backdrop-blur-xl border-gray-200/80',
        'dark:bg-[rgba(28,19,21,0.85)] dark:backdrop-blur-xl dark:border-white/[0.08]',
        collapsed ? 'w-16' : 'w-56',
      )}
    >
      {/* Brand */}
      <div
        className={cn(
          'flex items-center px-4 h-14 border-b transition-colors shrink-0',
          'border-gray-200/80 dark:border-white/[0.08]',
          collapsed ? 'justify-center' : 'gap-0',
        )}
      >
        {collapsed ? (
          <span className="text-base font-bold tracking-tighter text-gray-900 dark:text-white">
            AB
          </span>
        ) : (
          <span className="text-base font-bold tracking-tight text-gray-900 dark:text-white">
            Agent<span className="text-accent">-</span>Bridge
          </span>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-4 space-y-1 px-2 overflow-y-auto">
        {navItems.map(({ key, label, path, icon: Icon }) => (
          <NavLink
            key={key}
            to={path}
            end={path === '/'}
            className={({ isActive }) =>
              cn(
                'flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-all duration-200',
                isActive
                  ? 'bg-accent/12 text-accent ring-1 ring-accent/25'
                  : 'text-gray-600 dark:text-gray-400 hover:bg-gray-100/80 dark:hover:bg-white/[0.06] hover:text-gray-900 dark:hover:text-white',
              )
            }
          >
            <Icon size={18} className="shrink-0" />
            {!collapsed && <span>{label}</span>}
          </NavLink>
        ))}
      </nav>

      {/* Collapse toggle */}
      <div className={cn('border-t p-2', 'border-gray-200/80 dark:border-white/[0.08]')}>
        <button
          type="button"
          onClick={() => setCollapsed(!collapsed)}
          className={cn(
            'flex items-center justify-center w-full px-3 py-2 rounded-xl transition-colors duration-200',
            'text-gray-400 hover:bg-gray-100/80 dark:hover:bg-white/[0.06]',
          )}
        >
          {collapsed ? <ChevronRight size={18} /> : <ChevronLeft size={18} />}
        </button>
      </div>
    </aside>
  );
}
