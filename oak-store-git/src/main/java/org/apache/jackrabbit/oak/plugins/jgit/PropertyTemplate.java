package org.apache.jackrabbit.oak.plugins.jgit;

import org.apache.jackrabbit.oak.api.Type;

public interface PropertyTemplate {

	int getIndex();

	Type<?> getType();

}
