package com.vector.service.zip.crc;

interface Checksum {
	void update(byte[] buf, int index, int len);
	void reset();
	void reset(long init);
	long getValue();
	Checksum copy();
}