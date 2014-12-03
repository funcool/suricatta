package suricatta.impl;


public final class VisitListenerWrapper extends org.jooq.impl.DefaultVisitListener {
  private final IVisitListener listener;

  public VisitListenerWrapper(final IVisitListener listener) {
    this.listener = listener;
  }

  @Override
  public void visitStart(org.jooq.VisitContext context) {
    this.listener.start(context);
  }

  @Override
  public void visitEnd(org.jooq.VisitContext context) {
    this.listener.end(context);
  }
}


