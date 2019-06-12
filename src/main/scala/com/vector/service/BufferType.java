package com.vector.service;

import java.nio.ByteBuffer;

public enum BufferType
{
	ON_HEAP
			{
				public ByteBuffer allocate(int size)
				{
					return ByteBuffer.allocate(size);
				}
			},
	OFF_HEAP
			{
				public ByteBuffer allocate(int size)
				{
					return ByteBuffer.allocateDirect(size);
				}
			};

	public abstract ByteBuffer allocate(int size);

	public static BufferType typeOf(ByteBuffer buffer)
	{
		return buffer.isDirect() ? OFF_HEAP : ON_HEAP;
	}
}