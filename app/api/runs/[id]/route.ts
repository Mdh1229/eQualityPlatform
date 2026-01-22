import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest, { params }: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await params;
    
    const run = await prisma?.analysisRun?.findUnique({
      where: { id: id ?? '' },
      include: {
        results: true
      }
    });
    
    if (!run) {
      return NextResponse.json({ error: 'Run not found' }, { status: 404 });
    }
    
    return NextResponse.json({ run });
  } catch (error) {
    console.error('Fetch run error:', error);
    return NextResponse.json({ error: 'Failed to fetch run' }, { status: 500 });
  }
}
