/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: { extend: { colors: { canvas: 'var(--canvas)', surface: 'var(--surface)', ink: 'var(--ink)', muted: 'var(--muted)', line: 'var(--line)', accent: 'var(--accent)', good: 'var(--good)', warning: 'var(--warning)', critical: 'var(--critical)' } } },
  plugins: [],
}
