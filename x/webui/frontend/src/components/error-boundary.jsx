import { Component } from 'preact';

export class ErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    // Update state so the next render will show the fallback UI.
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    // Here you could log the error to an error reporting service
    // like Sentry, LogRocket, etc.
    console.error("ErrorBoundary caught an error:", error, errorInfo);
  }

  // Allow resetting the error boundary's state from a parent component
  // This could be useful if the user can take an action to resolve the error.
  resetBoundary = () => {
    this.setState({ hasError: false, error: null });
  }

  render() {
    if (this.state.hasError) {
      // Render a custom fallback UI
      return (
        <div class="error-boundary-fallback">
          <h2>Oops! Something went wrong.</h2>
          <p>
            An unexpected error occurred. Please try refreshing the page. We have been notified and will investigate the issue.
          </p>
          {
            // Optionally show error details in development
            process.env.NODE_ENV === 'development' && this.state.error && (
              <details>
                <summary>Error Details</summary>
                <pre>
                  {this.state.error.toString()}
                  {this.state.error.stack}
                </pre>
              </details>
            )}
        </div>
      );
    }

    return this.props.children;
  }
}
