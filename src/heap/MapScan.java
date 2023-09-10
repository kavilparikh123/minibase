package heap;

import java.io.IOException;

import BigT.Map;
import diskmgr.Page;
import global.GlobalConst;
import global.MID;
import global.PageId;
import global.RID;
import global.SystemDefs;

public class MapScan implements GlobalConst {

	private Heapfile _hf;

	private PageId dirpageId = new PageId();

	private HFPage dirpage = new HFPage();

	private RID datapageRid = new RID();

	private PageId datapageId = new PageId();

	private HFPage datapage = new HFPage();

	private MID userMid = new MID();

	private boolean nextUserStatus;

	public MapScan(Heapfile hf) throws InvalidTupleSizeException, IOException {
		init(hf);
	}

	public Map getNext(MID mid) throws InvalidTupleSizeException, IOException {
		Map recptrmap = null;
		if (nextUserStatus != true) {
			nextDataPage();
		}
		if (datapage == null)
			return null;
		mid.setPageNo(userMid.getPageNo());
		mid.setSlotNo(userMid.getSlotNo());
		try {
			recptrmap = datapage.getMap(mid);
		} catch (Exception e) {
			e.printStackTrace();
		}
		userMid = datapage.nextMap(mid);
		if (userMid == null)
			nextUserStatus = false;
		else
			nextUserStatus = true;
		return recptrmap;
	}

	public boolean position(MID mid) throws InvalidTupleSizeException, IOException {
		MID nxtmid = new MID();
		boolean bst;
		bst = peekNext(nxtmid);
		if (nxtmid.equals(mid) == true)
			return true;
		PageId pgid = new PageId();
		pgid.pid = mid.getPageNo().pid;
		if (!datapageId.equals(pgid)) {
			reset();
			bst = firstDataPage();
			if (bst != true)
				return bst;
			while (!datapageId.equals(pgid)) {
				bst = nextDataPage();
				if (bst != true)
					return bst;
			}
		}
		try {
			userMid = datapage.firstMap();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (userMid == null) {
			bst = false;
			return bst;
		}
		bst = peekNext(nxtmid);
		while ((bst == true) && (nxtmid != mid))
			bst = mvNext(nxtmid);
		return bst;
	}

	private void init(Heapfile hf) throws InvalidTupleSizeException, IOException {
		_hf = hf;
		firstDataPage();
	}

	private boolean firstDataPage() throws InvalidTupleSizeException, IOException {
		DataPageInfo dpinfo;
		Tuple rectuple = null;
		Boolean bst;

		dirpageId.pid = _hf._firstDirPageId.pid;
		nextUserStatus = true;

		try {
			dirpage = new HFPage();
			pinPage(dirpageId, (Page) dirpage, false);
		} catch (Exception e) {
			e.printStackTrace();
		}

		datapageRid = dirpage.firstRecord();
		if (datapageRid != null) {

			try {
				rectuple = dirpage.getRecord(datapageRid);
			} catch (Exception e) {
				e.printStackTrace();
			}
			dpinfo = new DataPageInfo(rectuple);
			datapageId.pid = dpinfo.pageId.pid;
		} else {

			PageId nextDirPageId = new PageId();
			nextDirPageId = dirpage.getNextPage();
			if (nextDirPageId.pid != INVALID_PAGE) {
				try {
					unpinPage(dirpageId, false);
					dirpage = null;
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					dirpage = new HFPage();
					pinPage(nextDirPageId, (Page) dirpage, false);
				} catch (Exception e) {
					e.printStackTrace();
				}

				try {
					datapageRid = dirpage.firstRecord();
				} catch (Exception e) {
					e.printStackTrace();
					datapageId.pid = INVALID_PAGE;
				}
				if (datapageRid != null) {
					try {
						rectuple = dirpage.getRecord(datapageRid);
					} catch (Exception e) {
						e.printStackTrace();
					}
					if (rectuple.getLength() != DataPageInfo.size)
						return false;
					dpinfo = new DataPageInfo(rectuple);
					datapageId.pid = dpinfo.pageId.pid;
				} else {
					datapageId.pid = INVALID_PAGE;
				}
			} else {
				datapageId.pid = INVALID_PAGE;
			}
		}
		datapage = null;
		try {
			nextDataPage();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;

	}

	protected boolean nextDataPage() throws InvalidTupleSizeException, IOException {
		DataPageInfo dpinfo;
		boolean nextDataPageStatus;
		PageId nextDirPageId = new PageId();
		Tuple rectuple = null;
		if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
			return false;
		if (datapage == null) {
			if (datapageId.pid == INVALID_PAGE) {
				try {
					unpinPage(dirpageId, false);
					dirpage = null;
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				try {
					datapage = new HFPage();
					pinPage(datapageId, (Page) datapage, false);
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					userMid = datapage.firstMap();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return true;
			}
		}
		try {
			unpinPage(datapageId, false);
			datapage = null;
		} catch (Exception e) {
		}
		if (dirpage == null) {
			return false;
		}
		datapageRid = dirpage.nextRecord(datapageRid);
		if (datapageRid == null) {
			nextDataPageStatus = false;
			nextDirPageId = dirpage.getNextPage();
			try {
				unpinPage(dirpageId, false);
				dirpage = null;
				datapageId.pid = INVALID_PAGE;
			} catch (Exception e) {
			}
			if (nextDirPageId.pid == INVALID_PAGE)
				return false;
			else {
				dirpageId = nextDirPageId;
				try {
					dirpage = new HFPage();
					pinPage(dirpageId, (Page) dirpage, false);
				} catch (Exception e) {
				}
				if (dirpage == null)
					return false;
				try {
					datapageRid = dirpage.firstRecord();
					nextDataPageStatus = true;
				} catch (Exception e) {
					nextDataPageStatus = false;
					return false;
				}
			}
		}
		try {
			rectuple = dirpage.getRecord(datapageRid);
		} catch (Exception e) {
			System.err.println("HeapFile: Error in Scan" + e);
		}
		if (rectuple.getLength() != DataPageInfo.size)
			return false;
		dpinfo = new DataPageInfo(rectuple);
		datapageId.pid = dpinfo.pageId.pid;
		try {
			datapage = new HFPage();
			pinPage(dpinfo.pageId, (Page) datapage, false);
		} catch (Exception e) {
			System.err.println("HeapFile: Error in Scan" + e);
		}
		userMid = datapage.firstMap();
		if (userMid == null) {
			nextUserStatus = false;
			return false;
		}
		return true;
	}

	private boolean peekNext(MID mid) {
		mid.setPageNo(userMid.getPageNo());
		mid.setSlotNo(userMid.getSlotNo());
		return true;
	}

	private boolean mvNext(MID mid) throws InvalidTupleSizeException, IOException {
		MID nextmid;
		boolean status;
		if (datapage == null)
			return false;
		nextmid = datapage.nextMap(mid);
		if (nextmid != null) {
			userMid.setPageNo(nextmid.getPageNo());
			userMid.setSlotNo(nextmid.getSlotNo());
			return true;
		} else {
			status = nextDataPage();
			if (status == true) {
				mid.setPageNo(userMid.getPageNo());
				mid.setSlotNo(userMid.getSlotNo());
			}
		}
		return true;
	}

	public void closescan() {
		reset();
	}

	private void reset() {
		if (datapage != null) {
			try {
				unpinPage(datapageId, false);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		datapageId.pid = 0;
		datapage = null;
		if (dirpage != null) {
			try {
				unpinPage(dirpageId, false);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		dirpage = null;
		nextUserStatus = true;
	}

	private void pinPage(PageId pageno, Page page, boolean emptyPage) throws HFBufMgrException {
		try {
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Scan.java: pinPage() failed");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty) throws HFBufMgrException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Scan.java: unpinPage() failed");
		}
	}

	public boolean getNextUserStatus() {
		return nextUserStatus;
	}

	public void setNextUserStatus(boolean userStatus) {
		nextUserStatus = userStatus;
	}

	public HFPage getDataPage() {
		return datapage;
	}

	public MID getUserId() {
		return userMid;
	}

	public void setUserId(MID mid) {
		userMid = mid;
	}
}
