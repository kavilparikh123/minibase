package btree;

import global.PageId;

/**
 * IndexData: It extends the DataClass. It defines the data "pageNo" for index
 * node in B++ tree.
 */
public class IndexData extends DataClass {
	private PageId pageId;

	/**
	 * Class constructor
	 * 
	 * @param pageNo the page number
	 */
	IndexData(int pageNo) {
		pageId = new PageId(pageNo);
	}

	/**
	 * Class constructor
	 * 
	 * @param pageNo the page number
	 */
	IndexData(PageId pageNo) {
		pageId = new PageId(pageNo.pid);
	};

	/**
	 * get a copy of the pageNo
	 * 
	 * @return the reference of the copy
	 */
	protected PageId getData() {
		return new PageId(pageId.pid);
	};

	/**
	 * set the pageNo
	 */
	protected void setData(PageId pageNo) {
		pageId = new PageId(pageNo.pid);
	};

	public String toString() {
		return (new Integer(pageId.pid)).toString();
	};
}
