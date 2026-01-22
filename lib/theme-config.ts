// eMAX Theme Configuration - Based on Enhanced_Excel_Theme.js

export const brandColors = {
  excelGreen: '#D7FF32',
  excelPurple: '#BEA0FE',
  excelOrange: '#FF7863',
  excelBlack: '#141414',
  excelGrey: '#AAAAAF',
  excelWhite: '#F5F5F5'
};

export const darkTheme = {
  colors: {
    background: {
      primary: '#141414',
      secondary: '#1a1a1a',
      tertiary: '#1f1f1f',
      elevated: '#242424',
      card: '#1f1f1f',
    },
    text: {
      primary: '#F5F5F5',
      secondary: '#AAAAAF',
      tertiary: '#808085',
    },
    border: '#2a2a2a',
    status: {
      success: '#D7FF32',
      warning: '#FF7863',
      error: '#FF4444',
      info: '#BEA0FE',
      pause: '#FF7863',
    },
    table: {
      header: '#1a1a1a',
      row: '#1f1f1f',
      rowHover: '#242424',
      rowAlt: '#1a1a1a',
    },
    action: {
      promote: { bg: 'rgba(215, 255, 50, 0.15)', border: '#D7FF32', text: '#D7FF32' },
      demote: { bg: 'rgba(255, 120, 99, 0.15)', border: '#FF7863', text: '#FF7863' },
      below: { bg: 'rgba(255, 68, 68, 0.15)', border: '#FF4444', text: '#FF4444' },
      correct: { bg: 'rgba(190, 160, 254, 0.15)', border: '#BEA0FE', text: '#BEA0FE' },
      review: { bg: 'rgba(170, 170, 175, 0.15)', border: '#AAAAAF', text: '#AAAAAF' },
      pause: { bg: 'rgba(255, 120, 99, 0.2)', border: '#FF7863', text: '#FF7863' },
      insufficient_volume: { bg: 'rgba(255, 193, 7, 0.15)', border: '#FFC107', text: '#FFC107' },
    }
  },
  shadows: {
    card: '0 4px 6px rgba(0, 0, 0, 0.3)',
    elevated: '0 10px 25px rgba(0, 0, 0, 0.4)',
  },
  glows: {
    green: '0 0 20px rgba(215, 255, 50, 0.3)',
    purple: '0 0 20px rgba(190, 160, 254, 0.3)',
    orange: '0 0 20px rgba(255, 120, 99, 0.3)',
  }
};

export const lightTheme = {
  colors: {
    background: {
      primary: '#F5F5F5',
      secondary: '#FFFFFF',
      tertiary: '#FAFAFA',
      elevated: '#FFFFFF',
      card: '#FFFFFF',
    },
    text: {
      primary: '#141414',
      secondary: '#666666',
      tertiary: '#AAAAAF',
    },
    border: '#E5E5E5',
    status: {
      success: '#4CAF50',
      warning: '#FF7863',
      error: '#F44336',
      info: '#764BA2',
      pause: '#FF7863',
    },
    table: {
      header: '#FAFAFA',
      row: '#FFFFFF',
      rowHover: '#F5F5F5',
      rowAlt: '#FAFAFA',
    },
    action: {
      promote: { bg: 'rgba(76, 175, 80, 0.1)', border: '#4CAF50', text: '#4CAF50' },
      demote: { bg: 'rgba(255, 120, 99, 0.1)', border: '#FF7863', text: '#E55A45' },
      below: { bg: 'rgba(244, 67, 54, 0.1)', border: '#F44336', text: '#F44336' },
      correct: { bg: 'rgba(118, 75, 162, 0.1)', border: '#764BA2', text: '#764BA2' },
      review: { bg: 'rgba(170, 170, 175, 0.1)', border: '#AAAAAF', text: '#666666' },
      pause: { bg: 'rgba(255, 120, 99, 0.15)', border: '#FF7863', text: '#E55A45' },
      insufficient_volume: { bg: 'rgba(255, 152, 0, 0.1)', border: '#FF9800', text: '#E65100' },
    }
  },
  shadows: {
    card: '0 2px 4px rgba(0, 0, 0, 0.05)',
    elevated: '0 4px 12px rgba(0, 0, 0, 0.08)',
  },
  glows: {
    green: '0 0 15px rgba(76, 175, 80, 0.2)',
    purple: '0 0 15px rgba(118, 75, 162, 0.2)',
    orange: '0 0 15px rgba(255, 120, 99, 0.2)',
  }
};

export type ThemeConfig = typeof darkTheme;
