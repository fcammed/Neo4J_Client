package com.vector.service.zip;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
//import java.util.zip.DeflaterOutputStream;
import com.vector.service.zip.original.DeflaterOutputStream;

public class ParallelDeflaterOutputStream extends DeflaterOutputStream {
	public boolean usesDefaultDeflater = false;

	public ParallelDeflaterOutputStream(OutputStream out, Deflater def) {
		super(out, def);
	}

	public void deflate() throws IOException {
		super.deflate();
	}
}
