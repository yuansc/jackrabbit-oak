/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.jgit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.checkValidName;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * A record of type "NODE". This class can read a node record from a segment. It
 * currently doesn't cache data (but the template is fully loaded).
 */
public class JGitNodeState extends Record implements NodeState {
    @Nonnull
    private final JGitReader reader;

    @Nullable
    private final BlobStore blobStore;

    @Nonnull
    private final Supplier<JGitWriter> writer;

    private volatile RecordId templateId = null;

    private volatile Template template = null;

    JGitNodeState(
            @Nonnull JGitReader reader,
            @Nonnull Supplier<JGitWriter> writer,
            @Nullable BlobStore blobStore,
            @Nonnull RecordId id) {
        super(id);
        this.reader = checkNotNull(reader);
        this.writer = checkNotNull(memoize(writer));
        this.blobStore = blobStore;
    }

    public JGitNodeState(
            @Nonnull JGitReader reader,
            @Nonnull JGitWriter writer,
            @Nullable BlobStore blobStore,
            @Nonnull RecordId id) {
        this(reader, Suppliers.ofInstance(writer), blobStore, id);
    }

    RecordId getTemplateId() {
        if (templateId == null) {
            // no problem if updated concurrently,
            // as each concurrent thread will just get the same value
            templateId = new RecordId();
        }
        return templateId;
    }

    private Object getRecordNumber() {
		// TODO Auto-generated method stub
		return null;
	}

	private Segment getSegment() {
		// TODO Auto-generated method stub
		return null;
	}

	Template getTemplate() {
        if (template == null) {
            // no problem if updated concurrently,
            // as each concurrent thread will just get the same value
            template = reader.readTemplate(getTemplateId());
        }
        return template;
    }

    MapRecord getChildNodeMap() {
    	return new MapRecord();
    }

    @Nonnull
    static String getStableId(@Nonnull ByteBuffer stableId) {
        ByteBuffer buffer = stableId.duplicate();
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        int offset = buffer.getInt();
        return new UUID(msb, lsb) + ":" + offset;
    }

    /**
     * Returns the stable id of this node. In contrast to the node's record id
     * (which is technically the node's address) the stable id doesn't change
     * after an online gc cycle. It might though change after an offline gc cycle.
     *
     * @return  stable id
     */
    public String getStableId() {
        return getStableId(getStableIdBytes());
    }

    /**
     * Returns the stable ID of this node, non parsed. In contrast to the node's
     * record id (which is technically the node's address) the stable id doesn't
     * change after an online gc cycle. It might though change after an offline
     * gc cycle.
     *
     * @return the stable ID of this node.
     */
    public ByteBuffer getStableIdBytes() {
        // The first record id of this node points to the stable id.
        RecordId id = new RecordId();
        return null;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public long getPropertyCount() {
        Template template = getTemplate();
        return 0;
    }

    @Override
    public boolean hasProperty(@Nonnull String name) {
        checkNotNull(name);
        Template template = getTemplate();
        switch (name) {
            case JCR_PRIMARYTYPE:
                return template.getPrimaryType() != null;
            case JCR_MIXINTYPES:
                return template.getMixinTypes() != null;
            default:
                return template.getPropertyTemplate(name) != null;
        }
    }

    @Override @CheckForNull
    public PropertyState getProperty(@Nonnull String name) {
        checkNotNull(name);
        Template template = getTemplate();
        PropertyState property = null;
        if (JCR_PRIMARYTYPE.equals(name)) {
            property = template.getPrimaryType();
        } else if (JCR_MIXINTYPES.equals(name)) {
            property = template.getMixinTypes();
        }
        if (property != null) {
            return property;
        }

        PropertyTemplate propertyTemplate =
                template.getPropertyTemplate(name);
        /*
        if (propertyTemplate != null) {
            Segment segment = getSegment();
            RecordId id = getRecordId(segment, template, propertyTemplate);
            return reader.readProperty(id, propertyTemplate);
        } else {
            return null;
        }
        */
        return null;
    }

    private RecordId getRecordId(Segment segment, Template template,
                                 PropertyTemplate propertyTemplate) {
    	return new RecordId();
    }

    @Override @Nonnull
    public Iterable<PropertyState> getProperties() {
        Template template = getTemplate();
        PropertyTemplate[] propertyTemplates = template.getPropertyTemplates();
        List<PropertyState> list =
                newArrayListWithCapacity(propertyTemplates.length + 2);
        return list;
    }

    @Override
    public boolean getBoolean(@Nonnull String name) {
        return Boolean.TRUE.toString().equals(getValueAsString(name, BOOLEAN));
    }

    @Override
    public long getLong(String name) {
        String value = getValueAsString(name, LONG);
        if (value != null) {
            return Long.parseLong(value);
        } else {
            return 0;
        }
    }

    @Override @CheckForNull
    public String getString(String name) {
        return getValueAsString(name, STRING);
    }

    @Override @Nonnull
    public Iterable<String> getStrings(@Nonnull String name) {
        return getValuesAsStrings(name, STRINGS);
    }

    @Override @CheckForNull
    public String getName(@Nonnull String name) {
        return getValueAsString(name, NAME);
    }

    @Override @Nonnull
    public Iterable<String> getNames(@Nonnull String name) {
        return getValuesAsStrings(name, NAMES);
    }

    /**
     * Optimized value access method. Returns the string value of a property
     * of a given non-array type. Returns {@code null} if the named property
     * does not exist, or is of a different type than given.
     *
     * @param name property name
     * @param type property type
     * @return string value of the property, or {@code null}
     */
    @CheckForNull
    private String getValueAsString(String name, Type<?> type) {
        checkArgument(!type.isArray());

        Template template = getTemplate();
        if (JCR_PRIMARYTYPE.equals(name)) {
            PropertyState primary = template.getPrimaryType();
            if (primary != null) {
                if (type == NAME) {
                    return primary.getValue(NAME);
                } else {
                    return null;
                }
            }
        } else if (JCR_MIXINTYPES.equals(name)
                && template.getMixinTypes() != null) {
            return null;
        }

        PropertyTemplate propertyTemplate =
                template.getPropertyTemplate(name);
        if (propertyTemplate == null
                || propertyTemplate.getType() != type) {
            return null;
        }

        Segment segment = (Segment) getSegment();
        RecordId id = getRecordId(segment, template, propertyTemplate);
        return reader.readString(id);
    }

    /**
     * Optimized value access method. Returns the string values of a property
     * of a given array type. Returns an empty iterable if the named property
     * does not exist, or is of a different type than given.
     *
     * @param name property name
     * @param type property type
     * @return string values of the property, or an empty iterable
     */
    @Nonnull
    private Iterable<String> getValuesAsStrings(String name, Type<?> type) {
        checkArgument(type.isArray());

        Template template = getTemplate();
        if (JCR_MIXINTYPES.equals(name)) {
            PropertyState mixin = template.getMixinTypes();
            if (type == NAMES && mixin != null) {
                return mixin.getValue(NAMES);
            } else if (type == NAMES || mixin != null) {
                return emptyList();
            }
        } else if (JCR_PRIMARYTYPE.equals(name)
                && template.getPrimaryType() != null) {
            return emptyList();
        }

        PropertyTemplate propertyTemplate =
                template.getPropertyTemplate(name);
        if (propertyTemplate == null
                || propertyTemplate.getType() != type) {
            return emptyList();
        }
        return emptyList();
    }

    @Override
    public long getChildNodeCount(long max) {
        return 0;
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        String childName = getTemplate().getChildName();
        return false;
    }

    @Override @Nonnull
    public NodeState getChildNode(@Nonnull String name) {
        return MISSING_NODE;
    }

    @Override @Nonnull
    public Iterable<String> getChildNodeNames() {
        String childName = getTemplate().getChildName();
        return Collections.singletonList(childName);
    }

    @Override @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        return Collections.emptyList();
    }

    @Override @Nonnull
    public JGitNodeBuilder builder() {
        return new JGitNodeBuilder(this, blobStore, reader, writer.get());
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
    	return true;
    }

    private static boolean compareProperties(
            PropertyState before, PropertyState after, NodeStateDiff diff) {
        if (before == null) {
            return after == null || diff.propertyAdded(after);
        } else if (after == null) {
            return diff.propertyDeleted(before);
        } else {
            return before.equals(after) || diff.propertyChanged(before, after);
        }
    }

    //------------------------------------------------------------< Object >--
    
    /**
     * Indicates whether two {@link NodeState} instances are equal to each
     * other. A return value of {@code true} clearly means that the instances
     * are equal, while a return value of {@code false} doesn't necessarily mean
     * the instances are not equal. These "false negatives" are an
     * implementation detail and callers cannot rely on them being stable.
     * 
     * @param a
     *            the first {@link NodeState} instance
     * @param b
     *            the second {@link NodeState} instance
     * @return {@code true}, if these two instances are equal.
     */
    public static boolean fastEquals(NodeState a, NodeState b) {
        return false;
    }

    @Override
    public int hashCode() {
        return getStableId().hashCode();
    }

    @Override
    public boolean equals(Object object) {
    	return false;
    }

    @Override
    public String toString() {
        return AbstractNodeState.toString(this);
    }

}
