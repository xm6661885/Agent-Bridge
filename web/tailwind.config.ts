import type { Config } from 'tailwindcss'

export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        // Warm, cute palette: grays are tinted toward rose/cream.
        gray: {
          50: '#fff8f5',
          100: '#fdeee9',
          200: '#f7dcd5',
          300: '#ecc4bb',
          400: '#cf9f96',
          500: '#a97c75',
          600: '#86605b',
          700: '#5f4442',
          800: '#3f2d2e',
          900: '#2a1e20',
          950: '#1c1315',
        },
        accent: {
          DEFAULT: 'rgb(var(--color-accent) / <alpha-value>)',
          dim: 'rgb(var(--color-accent-dim) / <alpha-value>)',
        },
      },
      borderRadius: {
        lg: '0.75rem',
        xl: '1rem',
        '2xl': '1.25rem',
      },
      animation: {
        'fade-in': 'fadeIn 0.4s ease',
        'slide-up': 'slideUp 0.4s ease',
        'float-in': 'floatIn 0.4s ease',
      },
      keyframes: {
        fadeIn: {
          from: { opacity: '0', transform: 'scale(0.96) translateY(20px)' },
          to: { opacity: '1', transform: 'scale(1)' },
        },
        slideUp: {
          from: { opacity: '0', transform: 'translateY(10px)' },
          to: { opacity: '1', transform: 'translateY(0)' },
        },
        floatIn: {
          from: { opacity: '0', transform: 'translateY(10px)' },
          to: { opacity: '1', transform: 'translateY(0)' },
        },
      },
    },
  },
  plugins: [require('@tailwindcss/typography')],
} satisfies Config
