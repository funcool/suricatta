package suricatta.impl;

import org.jooq.RenderContext;
import org.jooq.BindContext;


public interface IParam {
  public Object render(RenderContext ctx);
  public Object bind(BindContext ctx);
}
