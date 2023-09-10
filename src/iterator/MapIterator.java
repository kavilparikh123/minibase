package iterator;

import java.io.IOException;

import BigT.Map;
import bufmgr.PageNotReadException;
import diskmgr.Page;
import global.Flags;
import global.PageId;
import global.SystemDefs;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import index.IndexException;

public abstract class MapIterator implements Flags {
	public boolean closeFlag = false;

	public abstract void close() throws IOException, JoinsException, SortException, IndexException;

	public void free_buffer_pages(int numPages, PageId[] pageIds) throws IteratorBMException {
		for (int i = 0; i < numPages; i++) {
			freePage(pageIds[i]);
		}
	}

	private void freePage(PageId pageId) throws IteratorBMException {
		try {
			SystemDefs.JavabaseBM.freePage(pageId);
		} catch (Exception e) {
			throw new IteratorBMException(e, "Iterator.java: freePage() failed");
		}
	}

	public void get_buffer_pages(int numPages, PageId[] pageIds, byte[][] buffers) throws IteratorBMException {
		Page pagePtr = new Page();
		PageId pageId = null;
		for (int i = 0; i < numPages; i++) {
			pagePtr.setpage(buffers[i]);
			pageId = newPage(pagePtr, 1);
			pageIds[i] = new PageId(pageId.pid);
			buffers[i] = pagePtr.getpage();
		}
	}

	public abstract Map get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception;

	private PageId newPage(Page page, int num) throws IteratorBMException {
		PageId tmpId = new PageId();
		try {
			tmpId = SystemDefs.JavabaseBM.newPage(page, num);
		} catch (Exception e) {
			throw new IteratorBMException(e, "Iterator.java: newPage() failed");
		}
		return tmpId;
	}
}