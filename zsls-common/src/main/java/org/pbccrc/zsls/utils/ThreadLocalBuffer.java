package org.pbccrc.zsls.utils;

import java.nio.CharBuffer;

import com.google.gson.Gson;

public class ThreadLocalBuffer {

	static final int INIT_BUFFER_LENGTH = 65536;
	static final int MAX_BUFFER_LENGTH = 1048576;
	
	static final int INIT_DATA_BUFFER_LENGTH = 2 * 1048576;
	static final int MAX_DATA_BUFFER_LENGTH = 5 * 1048576;
	
	// gson
	static ThreadLocal<Gson> gsonBuffer =
		new ThreadLocal<Gson>() {
			protected Gson initialValue() {
				return new Gson();
		}
	};
	public static Gson getGson() {
		return gsonBuffer.get();
	}
	
	// String Builder
	static ThreadLocal<StringBuilder> stringbuilder =
		new ThreadLocal<StringBuilder>() {
	        protected StringBuilder initialValue() {
	            return new StringBuilder(512);
        }
	};
	public static StringBuilder getLogBuilder(int thread) {
		StringBuilder builder = stringbuilder.get();
		builder.delete(0, builder.length());
		return builder;
	}
	static ThreadLocal<StringBuilder> stringbuilder2 =
		new ThreadLocal<StringBuilder>() {
	        protected StringBuilder initialValue() {
	            return new StringBuilder(INIT_BUFFER_LENGTH);
        }
	};
	public static StringBuilder getStringBuilder2(int thread) {
		StringBuilder builder = stringbuilder.get();
		builder.delete(0, builder.length());
		return builder;
	}
	
	// Byte Buffer
	static ThreadLocal<byte[]> bytebuffer =
		new ThreadLocal<byte[]>() {
	        protected byte[] initialValue() {
	            return new byte[INIT_BUFFER_LENGTH];
        }
	};
	public static byte[] getByteBuffer(int maxLength) {
		if (maxLength > MAX_BUFFER_LENGTH) {
			return new byte[maxLength];
		}
		byte[] buffer = bytebuffer.get();
		if (buffer.length < maxLength) {
			int length = buffer.length;
			while (length < maxLength && length < MAX_BUFFER_LENGTH)
				length *= 2;
			buffer = new byte[length];
			bytebuffer.set(buffer);
		}
		return buffer;
	}
	
	// Char Buffer
	static ThreadLocal<CharBuffer> wrapCharBuffer = new ThreadLocal<CharBuffer>() {
		protected CharBuffer initialValue() {
			return CharBuffer.allocate(INIT_DATA_BUFFER_LENGTH);
		}
	};
	public static CharBuffer getWrapCharBuffer(int length) {
		if (length > MAX_DATA_BUFFER_LENGTH)
			return CharBuffer.allocate(length);
		CharBuffer buff = wrapCharBuffer.get();
		buff.clear();
		if (buff.length() < length) {
			int len = buff.length();
			while (len < length)
				len *= 2;	
			wrapCharBuffer.set(CharBuffer.allocate(len));
		}
		return buff;
	}
	
	// Char Buffer
	static ThreadLocal<char[]> charbuffer =
		new ThreadLocal<char[]>() {
	        protected char[] initialValue() {
	            return new char[INIT_BUFFER_LENGTH];
        }
	};
	public static char[] getCharBuffer(int maxLength) {
		if (maxLength > MAX_BUFFER_LENGTH) {
			return new char[maxLength];
		}
		char[] buffer = charbuffer.get();
		if (buffer.length < maxLength) {
			int length = buffer.length;
			while (length < maxLength && length < MAX_BUFFER_LENGTH)
				length *= 2;
			buffer = new char[length];
			charbuffer.set(buffer);
		}
		return buffer;
	}

}
