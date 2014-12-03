package suricatta.impl;

import org.jooq.impl.DefaultDataType;
import org.jooq.RenderContext;
import org.jooq.BindContext;
import org.jooq.Context;
import org.jooq.DataType;


@SuppressWarnings("unchecked")
public class ParamWrapper extends org.jooq.impl.CustomField {
  private final IParam paramImpl;

  public ParamWrapper(final IParam paramImpl) {
    super(null, DefaultDataType.getDefaultDataType("__suricatta_other"));
    this.paramImpl = paramImpl;
  }

  @Override
  public void accept(Context ctx) {
    if (ctx instanceof RenderContext) {
      this.paramImpl.render((RenderContext) ctx);
    } else {
      this.paramImpl.bind((BindContext) ctx);
    }
  }
}


