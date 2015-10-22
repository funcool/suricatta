package suricatta.impl;

import org.jooq.RenderContext;
import org.jooq.BindContext;


public interface IParam {
  public Object render(Object value, RenderContext ctx);
  public Object bind(Object value, BindContext ctx);
}
