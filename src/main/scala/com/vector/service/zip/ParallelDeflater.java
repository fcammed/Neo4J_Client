package com.vector.service.zip;
import java.util.zip.Deflater;

public class ParallelDeflater extends Deflater{
	private long bytesRead;
	private long bytesWritten;
	public ParallelDeflater(int level, boolean nowrap) {
		super(level, nowrap);
	}

	public void setBytesRead(long bytesRead) {
		this.bytesRead = bytesRead;
	}

	public void setBytesWritten(long bytesWritten) {
		this.bytesWritten = bytesWritten;
	}
}
