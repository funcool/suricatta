package suricatta.impl;

import org.jooq.impl.DefaultDataType;
import org.jooq.conf.ParamType;
import org.jooq.RenderContext;
import org.jooq.BindContext;
import org.jooq.Context;
import org.jooq.DataType;
import org.jooq.Param;
import org.jooq.ParamMode;

import static org.jooq.conf.ParamType.INDEXED;
import static org.jooq.conf.ParamType.INLINED;


@SuppressWarnings({"unchecked", "deprecation"})
public class ParamWrapper extends org.jooq.impl.CustomField
  implements Param {

  private final IParam adapter;
  private boolean inline;
  private Object value;

  public ParamWrapper(final IParam adapter, final Object value) {
    super(null, DefaultDataType.getDefaultDataType("__suricatta_other"));
    this.adapter = adapter;
    this.value = value;
    this.inline = false;
  }

  @Override
  public ParamMode getParamMode() {
    return ParamMode.IN;
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public String getParamName() {
    return null;
  }

  @Override
  public final ParamType getParamType() {
    return inline ? INLINED: INDEXED;
  }

  @Override
  public void setValue(final Object val) {
    this.setConverted(val);
  }

  @Override
  public void setConverted(final Object val) {
    this.value = getDataType().convert(val);
  }

  @Override
  public void setInline(final boolean inline) {
    this.inline = inline;
  }

  @Override
  public boolean isInline() {
    return this.inline;
  }

  @Override
  public void accept(Context ctx) {
    if (ctx instanceof RenderContext) {
      this.adapter.render(this.value, (RenderContext) ctx);
    } else {
      this.adapter.bind(this.value, (BindContext) ctx);
    }
  }
}


