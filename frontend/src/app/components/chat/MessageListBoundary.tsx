import React from 'react';

interface MessageListBoundaryProps {
  title: string;
  description: string;
  retryLabel: string;
  children: React.ReactNode;
}

interface MessageListBoundaryState {
  hasError: boolean;
}

export class MessageListBoundary extends React.Component<MessageListBoundaryProps, MessageListBoundaryState> {
  constructor(props: MessageListBoundaryProps) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(): MessageListBoundaryState {
    return { hasError: true };
  }

  private handleRetry = () => {
    this.setState({ hasError: false });
  };

  render() {
    if (this.state.hasError) {
      return (
        <div className="rounded-2xl border border-error/25 bg-error/5 px-4 py-3 text-sm text-error">
          <div className="font-semibold">{this.props.title}</div>
          <div className="mt-1 text-xs text-error/90">{this.props.description}</div>
          <button
            onClick={this.handleRetry}
            className="mt-3 rounded-full border border-error/35 px-3 py-1 text-xs font-semibold text-error transition hover:bg-error/10"
          >
            {this.props.retryLabel}
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}
