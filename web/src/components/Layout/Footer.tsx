export default function Footer() {
  const year = new Date().getFullYear();

  return (
    <footer className="shrink-0 mt-4 pt-3 pb-1 text-center text-xs text-gray-400 dark:text-gray-500 select-none">
      <span>© {year} Agent-Bridge</span>
    </footer>
  );
}
