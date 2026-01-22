import { NextResponse } from 'next/server';
import { prisma } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const runs = await prisma?.analysisRun?.findMany({
      orderBy: { createdAt: 'desc' },
      take: 10,
      select: {
        id: true,
        name: true,
        startDate: true,
        endDate: true,
        fileName: true,
        totalRecords: true,
        promoteCount: true,
        demoteCount: true,
        belowMinCount: true,
        correctCount: true,
        reviewCount: true,
        createdAt: true
      }
    });
    
    return NextResponse.json({ runs: runs ?? [] });
  } catch (error) {
    console.error('Fetch runs error:', error);
    return NextResponse.json({ runs: [] });
  }
}
