package com.vector.service;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

public interface ICompressor {
	public int initialCompressedBufferLength(int chunkLength);

	public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException;

	/**
	 * Compression for ByteBuffers.
	 * <p>
	 * The data between input.position() and input.limit() is compressed and placed into output starting from output.position().
	 * Positions in both buffers are moved to reflect the bytes read and written. Limits are not changed.
	 */
	public void compress(ByteBuffer input, ByteBuffer output) throws IOException;

	/**
	 * Decompression for DirectByteBuffers.
	 * <p>
	 * The data between input.position() and input.limit() is uncompressed and placed into output starting from output.position().
	 * Positions in both buffers are moved to reflect the bytes read and written. Limits are not changed.
	 */
	public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException;

	/**
	 * Returns the preferred (most efficient) buffer type for this compressor.
	 */
	public BufferType preferredBufferType();

	/**
	 * Checks if the given buffer would be supported by the compressor. If a type is supported, the compressor must be
	 * able to use it in combination with all other supported types.
	 * <p>
	 * Direct and memory-mapped buffers must be supported by all compressors.
	 */
	public boolean supports(BufferType bufferType);

	public Set<String> supportedOptions();
}