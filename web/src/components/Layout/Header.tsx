import { useState } from 'react';
import {
  RefreshCw, Sun, Moon, Monitor, LogOut, ChevronDown,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { useThemeStore } from '@/store/theme';
import { useAuthStore } from '@/store/auth';

export default function Header() {
  const { theme, setTheme } = useThemeStore();
  const logout = useAuthStore((s) => s.logout);
  const [spinning, setSpinning] = useState(false);

  const handleRefresh = () => {
    setSpinning(true);
    window.dispatchEvent(new CustomEvent('agent-bridge:refresh'));
    setTimeout(() => setSpinning(false), 1000);
  };

  const themeIcons = { light: Sun, dark: Moon, system: Monitor };
  const nextTheme = { light: 'dark' as const, dark: 'system' as const, system: 'light' as const };
  const ThemeIcon = themeIcons[theme];

  const btnCls = cn(
    'p-2 rounded-lg transition-all duration-200',
    'text-gray-500 dark:text-gray-400',
    'hover:bg-gray-100/90 dark:hover:bg-white/[0.08] hover:text-gray-800 dark:hover:text-white',
  );

  return (
    <header
      className={cn(
        'h-14 flex items-center justify-end gap-1 px-4 shrink-0 relative z-20',
        'border-b border-gray-200/80 dark:border-white/[0.08]',
        'bg-white/70 backdrop-blur-xl dark:bg-[rgba(28,19,21,0.72)]',
      )}
    >
      <button type="button" onClick={handleRefresh} className={btnCls} aria-label={"Refresh"}>
        <RefreshCw size={16} className={spinning ? 'animate-spin' : ''} />
      </button>

      {/* Theme */}
      <button type="button" onClick={() => setTheme(nextTheme[theme])} className={btnCls} aria-label="Theme">
        <ThemeIcon size={16} />
      </button>

      {/* Logout */}
      <button
        type="button"
        onClick={logout}
        className={cn(
          'p-2 rounded-lg transition-all duration-200',
          'text-gray-400 hover:bg-red-500/10 hover:text-red-600 dark:hover:text-red-400',
        )}
        aria-label={"Log out"}
      >
        <LogOut size={16} />
      </button>
    </header>
  );
}
