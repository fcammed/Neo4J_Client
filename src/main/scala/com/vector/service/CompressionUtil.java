package com.vector.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

public class CompressionUtil {

	private static final String LOCAL_ENCODING = "UTF-8";
	private static final String ISO_ENCODING = "ISO-8859-1";
	private byte[] buffer = new byte[1024];
	Deflater zipDeflater = new Deflater(Deflater.BEST_SPEED,true);
	ByteArrayOutputStream stream = new ByteArrayOutputStream();

	public String gzipCompression(String input) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out);
		gzip.write(input.getBytes(LOCAL_ENCODING));
		gzip.close();
		String gzipString = out.toString(ISO_ENCODING);
		return new String(Base64.getEncoder().encode(gzipString.getBytes(LOCAL_ENCODING)), LOCAL_ENCODING);
	}

	public byte[] zipCompression(String data) throws UnsupportedEncodingException, IOException {
		//Deflater zipDeflater = new Deflater();
		//ByteArrayOutputStream stream = new ByteArrayOutputStream();
		try {
			zipDeflater.setInput(getBytes(data));
			zipDeflater.finish();
			//byte[] buffer = new byte[1024];
			int count = 0;
			while (!zipDeflater.finished()) {
				count = zipDeflater.deflate(buffer);
				stream.write(buffer, 0, count);
			}
			stream.reset();
			//return new String(Base64.getEncoder().encode(stream.toByteArray()), LOCAL_ENCODING);
			return stream.toByteArray();
		} finally {
			stream.close();
			zipDeflater.end();
		}
	}

	public byte[] getBytes(String data) throws UnsupportedEncodingException {
		return data.getBytes(LOCAL_ENCODING);
	}

}