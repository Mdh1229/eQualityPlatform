import { NextRequest, NextResponse } from 'next/server';
import { generateBigQuerySQL } from '@/lib/sql-generator';

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  try {
    const body = await request?.json();
    const { startDate, endDate } = body ?? {};
    
    if (!startDate || !endDate) {
      return NextResponse.json({ error: 'Start date and end date are required' }, { status: 400 });
    }
    
    const sql = generateBigQuerySQL(startDate, endDate);
    return NextResponse.json({ sql });
  } catch (error) {
    console.error('SQL generation error:', error);
    return NextResponse.json({ error: 'Failed to generate SQL' }, { status: 500 });
  }
}
