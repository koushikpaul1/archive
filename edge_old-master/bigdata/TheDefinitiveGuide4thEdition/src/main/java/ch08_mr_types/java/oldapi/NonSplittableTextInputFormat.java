package oldapi;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.TextInputFormat;

public class NonSplittableTextInputFormat extends TextInputFormat {
  
  protected boolean isSplitable(FileSystem fs, Path file) {
    return false;
  }
}
