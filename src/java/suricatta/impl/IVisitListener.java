package suricatta.impl;

import org.jooq.VisitContext;

public interface IVisitListener {
  public Object start(VisitContext context);
  public Object end(VisitContext context);
}
