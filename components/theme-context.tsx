'use client';

import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { darkTheme, lightTheme, ThemeConfig } from '@/lib/theme-config';

interface ThemeContextType {
  isDark: boolean;
  theme: ThemeConfig;
  toggleTheme: () => void;
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [isDark, setIsDark] = useState(true);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    const saved = localStorage.getItem('theme');
    if (saved) {
      setIsDark(saved === 'dark');
    }
  }, []);

  useEffect(() => {
    if (mounted) {
      localStorage.setItem('theme', isDark ? 'dark' : 'light');
      document.documentElement.setAttribute('data-theme', isDark ? 'dark' : 'light');
    }
  }, [isDark, mounted]);

  const toggleTheme = () => setIsDark(!isDark);
  const theme = isDark ? darkTheme : lightTheme;

  if (!mounted) {
    return <div style={{ background: '#141414', minHeight: '100vh' }}>{children}</div>;
  }

  return (
    <ThemeContext.Provider value={{ isDark, theme, toggleTheme }}>
      <div 
        style={{ 
          background: theme.colors.background.primary,
          minHeight: '100vh',
          color: theme.colors.text.primary,
          transition: 'background-color 0.3s ease, color 0.3s ease'
        }}
      >
        {children}
      </div>
    </ThemeContext.Provider>
  );
}

export function useTheme() {
  const context = useContext(ThemeContext);
  if (!context) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
}
