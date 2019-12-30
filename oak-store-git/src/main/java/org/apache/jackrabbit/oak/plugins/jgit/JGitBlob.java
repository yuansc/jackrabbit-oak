package org.apache.jackrabbit.oak.plugins.jgit;

import java.io.InputStream;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

public class JGitBlob implements Blob {

	public JGitBlob(BlobStore blobStore, RecordId writeStream) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getContentIdentity() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getNewStream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getReference() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long length() {
		// TODO Auto-generated method stub
		return 0;
	}

}
