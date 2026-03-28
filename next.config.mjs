/**
 * Next.js Configuration File (ESM Format)
 * 
 * This configuration file sets up the Next.js application with:
 * - Build and runtime settings preservation from original next.config.js
 * - FastAPI backend proxy rewrites for the Quality Compass system
 * 
 * The proxy pattern allows the frontend to communicate with the FastAPI backend
 * through /backend-api/* routes while preserving existing API contracts.
 * 
 * @see Section 0.4.1 and 0.4.3 of the Agent Action Plan for proxy requirements
 */

import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

/**
 * ESM equivalent of __dirname
 * In ESM modules, __dirname is not available by default.
 * We derive it from import.meta.url to maintain compatibility
 * with the outputFileTracingRoot configuration.
 */
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/** @type {import('next').NextConfig} */
const nextConfig = {
  /**
   * Custom build output directory
   * Configurable via NEXT_DIST_DIR environment variable
   * Defaults to '.next' if not specified
   */
  distDir: process.env.NEXT_DIST_DIR || '.next',

  /**
   * Output mode configuration
   * Configurable via NEXT_OUTPUT_MODE environment variable
   * Supports 'standalone', 'export', or undefined for default behavior
   */
  output: process.env.NEXT_OUTPUT_MODE,

  /**
   * Experimental features configuration
   * outputFileTracingRoot is set to parent directory for proper
   * file tracing in monorepo or nested project structures
   */
  experimental: {
    outputFileTracingRoot: path.join(__dirname, '../'),
  },

  /**
   * ESLint configuration during builds
   * Set to true to ignore ESLint errors during production builds
   * This allows builds to complete even with linting warnings
   */
  eslint: {
    ignoreDuringBuilds: true,
  },

  /**
   * TypeScript configuration during builds
   * Set to false to ensure TypeScript errors fail the build
   * This enforces type safety in production deployments
   */
  typescript: {
    ignoreBuildErrors: false,
  },

  /**
   * Image optimization configuration
   * Set to unoptimized: true for static export compatibility
   * or when using external image optimization services
   */
  images: { unoptimized: true },

  /**
   * URL rewrites configuration for FastAPI backend proxy
   * 
   * This enables the Next.js frontend to proxy requests to the FastAPI backend
   * through the /backend-api/* path pattern. This preserves existing frontend
   * API contracts while allowing the backend to be served from a separate process.
   * 
   * Pattern: /backend-api/:path* -> FASTAPI_URL/:path*
   * 
   * Examples:
   * - /backend-api/runs -> http://localhost:8000/runs
   * - /backend-api/runs/123/compute -> http://localhost:8000/runs/123/compute
   * - /backend-api/actions -> http://localhost:8000/actions
   * 
   * The FASTAPI_URL environment variable controls the backend destination.
   * Defaults to 'http://localhost:8000' for local development.
   * 
   * @returns {Promise<Array<{source: string, destination: string}>>} Rewrite rules
   */
  async rewrites() {
    return [
      {
        source: '/backend-api/:path*',
        destination: `${process.env.FASTAPI_URL || 'http://localhost:8000'}/:path*`,
      },
    ];
  },
};

export default nextConfig;
