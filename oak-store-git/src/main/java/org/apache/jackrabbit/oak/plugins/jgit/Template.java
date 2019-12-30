package org.apache.jackrabbit.oak.plugins.jgit;

import org.apache.jackrabbit.oak.api.PropertyState;

public interface Template {

	PropertyState getPrimaryType();

	PropertyState getMixinTypes();

	PropertyTemplate getPropertyTemplate(String name);

	String getChildName();

	PropertyTemplate[] getPropertyTemplates();

}
