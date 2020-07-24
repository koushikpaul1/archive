package groupingComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Stock key. This key is a composite key. The "natural"
 * key is the symbol. The secondary sort will be performed
 * against the timestamp.
 * @author Jee Vang
 *
 */
public class StockKey implements WritableComparable<StockKey> {

	private String word;
	private String key;
	
	
	/**
	 * Constructor.
	 */
	public StockKey() { }
	
	public StockKey(String symbol) {
		this.word = symbol;
		
	}
	
	@Override
	public String toString() {
		return (new StringBuilder())				
				.append(word)
				.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		key= WritableUtils.readString(in).substring(0, 1);
		//word = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, key);
		//WritableUtils.writeString(out, word);
		
		
	}

	@Override
	public int compareTo(StockKey o) {
		int result = word.substring(0, 1).compareTo(o.word.substring(0, 1));		
		return result;
	}

	/**
	 * Gets the symbol.
	 * @return Symbol.
	 */
	public String getSymbol() {
		key="";
		key+= word.substring(0, 1);
		return key;
	}

	public void setSymbol(String word) {
		this.word = word;
	}



}
