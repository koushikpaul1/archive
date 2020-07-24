package groupingComperator;

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
	
	/**
	 * Constructor.
	 * @param symbol Stock symbol. i.e. APPL
	 * @param timestamp Timestamp. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 */
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
		word = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, word);
	}

	@Override
	public int compareTo(StockKey o) {
		int result = word.compareTo(o.word);		
		return result;
	}

	/**
	 * Gets the symbol.
	 * @return Symbol.
	 */
	public String getSymbol() {
		key="";
		key+= word.toUpperCase().charAt(0);
		return key;
	}

	public void setSymbol(String word) {
		this.word = word;
	}



}
