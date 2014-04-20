package Crawler_HtmlParser;

import org.htmlparser.Parser;
import org.htmlparser.Tag;
import org.htmlparser.visitors.NodeVisitor;

public class Page_Parser {
	public static void main(String[] args)throws Exception{ 
		Parser parser = new Parser();
		parser.setURL("http://www.google.com");
	    parser.setEncoding(parser.getEncoding());
	    System.out.println(parser.getEncoding());
	    NodeVisitor visitor = new NodeVisitor() {
	        public void visitTag(Tag tag) {
	        	System.out.println("tag name is "+tag.getTagName()+
	        			"classis"+tag.getClass());
	        }
	    };
	}

}
