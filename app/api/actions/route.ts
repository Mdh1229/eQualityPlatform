import { NextRequest, NextResponse } from 'next/server';
import { prisma } from '@/lib/db';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const {
      subId,
      vertical,
      trafficType,
      mediaType,
      actionTaken,
      actionLabel,
      previousState,
      newState,
      metricMode,
      callQuality,
      leadQuality,
      totalRevenue,
      notes,
      takenBy
    } = body;

    if (!subId || !actionTaken) {
      return NextResponse.json(
        { error: 'subId and actionTaken are required' },
        { status: 400 }
      );
    }

    const actionRecord = await prisma.actionHistory.create({
      data: {
        subId,
        vertical: vertical || '',
        trafficType: trafficType || '',
        mediaType: mediaType || null,
        actionTaken,
        actionLabel: actionLabel || actionTaken,
        previousState,
        newState,
        metricMode,
        callQuality: callQuality ?? null,
        leadQuality: leadQuality ?? null,
        totalRevenue: totalRevenue ?? null,
        notes,
        takenBy
      }
    });

    return NextResponse.json({ success: true, action: actionRecord });
  } catch (error) {
    console.error('Error recording action:', error);
    return NextResponse.json(
      { error: 'Failed to record action' },
      { status: 500 }
    );
  }
}

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const subId = searchParams.get('subId');
    const limit = parseInt(searchParams.get('limit') || '50');

    if (subId) {
      // Get history for a specific sub_id
      const history = await prisma.actionHistory.findMany({
        where: { subId },
        orderBy: { createdAt: 'desc' },
        take: limit
      });
      return NextResponse.json({ history });
    } else {
      // Get recent actions across all sub_ids
      const history = await prisma.actionHistory.findMany({
        orderBy: { createdAt: 'desc' },
        take: limit
      });
      return NextResponse.json({ history });
    }
  } catch (error) {
    console.error('Error fetching action history:', error);
    return NextResponse.json(
      { error: 'Failed to fetch action history' },
      { status: 500 }
    );
  }
}
