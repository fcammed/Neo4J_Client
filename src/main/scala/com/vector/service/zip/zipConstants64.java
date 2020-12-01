package com.vector.service.zip;

public class zipConstants64 {
	public static final long ZIP64_ENDSIG = 0x06064b50L;  // "PK\006\006"
	public static final long ZIP64_LOCSIG = 0x07064b50L;  // "PK\006\007"
	public static final int  ZIP64_ENDHDR = 56;           // ZIP64 end header size
	public static final int  ZIP64_LOCHDR = 20;           // ZIP64 end loc header size
	public static final int  ZIP64_EXTHDR = 24;           // EXT header size
	public static final int  ZIP64_EXTID  = 0x0001;       // Extra field Zip64 header ID
	public static final int  ZIP64_MAGICCOUNT = 0xFFFF;
	public static final long ZIP64_MAGICVAL = 0xFFFFFFFFL;
	public static final int  ZIP64_ENDLEN = 4;       // size of zip64 end of central dir
	public static final int  ZIP64_ENDVEM = 12;      // version made by
	public static final int  ZIP64_ENDVER = 14;      // version needed to extract
	public static final int  ZIP64_ENDNMD = 16;      // number of this disk
	public static final int  ZIP64_ENDDSK = 20;      // disk number of start
	public static final int  ZIP64_ENDTOD = 24;      // total number of entries on this disk
	public static final int  ZIP64_ENDTOT = 32;      // total number of entries
	public static final int  ZIP64_ENDSIZ = 40;      // central directory size in bytes
	public static final int  ZIP64_ENDOFF = 48;      // offset of first CEN header
	public static final int  ZIP64_ENDEXT = 56;      // zip64 extensible data sector
	public static final int  ZIP64_LOCDSK = 4;       // disk number start
	public static final int  ZIP64_LOCOFF = 8;       // offset of zip64 end
	public static final int  ZIP64_LOCTOT = 16;      // total number of disks
	public static final int  ZIP64_EXTCRC = 4;       // uncompressed file crc-32 value
	public static final int  ZIP64_EXTSIZ = 8;       // compressed size, 8-byte
	public static final int  ZIP64_EXTLEN = 16;      // uncompressed size, 8-byte
	public static final int EFS = 0x800;       // If this bit is set the filename and
	// comment fields for this file must be
	// encoded using UTF-8.
	public static final int  EXTID_ZIP64 = 0x0001;    // Zip64
	public static final int  EXTID_NTFS  = 0x000a;    // NTFS
	public static final int  EXTID_UNIX  = 0x000d;    // UNIX
	public static final int  EXTID_EXTT  = 0x5455;    // Info-ZIP Extended Timestamp
	public static final int  EXTT_FLAG_LMT = 0x1;       // LastModifiedTime
	public static final int  EXTT_FLAG_LAT = 0x2;       // LastAccessTime
	public static final int  EXTT_FLAT_CT  = 0x4;       // CreationTime
	private zipConstants64() {}
}
