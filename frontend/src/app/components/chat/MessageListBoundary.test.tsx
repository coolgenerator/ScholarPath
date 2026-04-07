import React from 'react';
import { render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { MessageListBoundary } from './MessageListBoundary';

function BrokenChild() {
  throw new Error('broken child');
}

describe('MessageListBoundary', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('shows fallback UI when child rendering throws', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    render(
      <MessageListBoundary
        title="render-error"
        description="render-error-desc"
        retryLabel="retry-render"
      >
        <BrokenChild />
      </MessageListBoundary>,
    );

    expect(screen.getByText('render-error')).toBeInTheDocument();
    expect(screen.getByText('render-error-desc')).toBeInTheDocument();
    expect(screen.getByText('retry-render')).toBeInTheDocument();
  });
});
