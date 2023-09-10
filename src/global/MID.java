package global;

import java.io.IOException;
import java.io.Serializable;

public class MID implements Serializable {
	private static final long serialVersionUID = -1L;

	private int slotNo;
	private PageId pageNo = new PageId();

	public MID() {
	}

	public MID(PageId pageNo, int slotNo) {
		this.slotNo = slotNo;
		this.pageNo = pageNo;
	}

	public void copyMid(MID mid) {
		this.slotNo = mid.slotNo;
		this.pageNo = mid.pageNo;
	}

	public boolean equals(MID mid) {
		return this.pageNo.pid == mid.pageNo.pid && this.slotNo == mid.slotNo;
	}

	public PageId getPageNo() {
		return pageNo;
	}

	public int getSlotNo() {
		return slotNo;
	}

	public void setPageNo(PageId pageNo) {
		this.pageNo = pageNo;
	}

	public void setSlotNo(int slotNo) {
		this.slotNo = slotNo;
	}

	public String toString() {
		return "{'PageNo':" + this.pageNo.pid + ", 'SlotNo:'" + this.slotNo + "}";
	}

	public void writeTOByteArray(byte[] array, int offset) throws IOException {
		Convert.setIntValue(slotNo, offset, array);
		Convert.setIntValue(pageNo.pid, offset + 4, array);
	}
}