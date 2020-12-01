package com.vector.service.zip;

import java.io.OutputStream;
import java.util.zip.CRC32;
import java.util.zip.ZipOutputStream;
//import com.vector.service.zip.original.ZipOutputStream;

public class ParallelZipOutputStream extends ZipOutputStream {
	private CRC32 crc = new CRC32();
	public ParallelZipOutputStream(OutputStream out) {
		super(out);
	}

	public void setCrc(CRC32 crc) {
		this.crc = crc;
	}
}
