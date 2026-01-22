import dynamic from 'next/dynamic';

const ClassifierClient = dynamic(() => import('@/components/classifier-client'), {
  ssr: false,
  loading: () => (
    <div style={{
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      height: '100vh',
      background: '#f5f7fa'
    }}>
      <div style={{ textAlign: 'center' }}>
        <div style={{
          width: 48,
          height: 48,
          border: '4px solid #e0e0e0',
          borderTopColor: '#667eea',
          borderRadius: '50%',
          animation: 'spin 1s linear infinite',
          margin: '0 auto 16px'
        }} />
        <p style={{ color: '#666' }}>Loading Quality Tier Classifier...</p>
      </div>
      <style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  )
});

export default function Home() {
  return <ClassifierClient />;
}
