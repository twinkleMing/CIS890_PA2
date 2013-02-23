/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * <p>
 * WritableComparable extension of a Java ArrayList. Elements in the list must
 * be homogeneous and must implement Hadoop's WritableComparable interface. This
 * class, combined with {@link Tuple}, allows the user to define arbitrarily
 * complex data structures.
 * </p>
 * 
 * @see Tuple
 * @param <E>
 *            type of list element
 * 
 * @author Jimmy Lin
 * @author Tamer Elsayed
 */
public class DoubleORPairDoubleArrayList<E extends Writable> implements Writable {
  private static final long serialVersionUID = 1L;
  private boolean isDouble = false;
  private double thisDouble;
  private PairOfDoubleArrayList thispairOfDA ;
  

  public DoubleORPairDoubleArrayList()
  {
  }

  public DoubleORPairDoubleArrayList(PairOfDoubleArrayList array)
  {
	  thispairOfDA = new PairOfDoubleArrayList(array);
      isDouble = false;
  }
  public DoubleORPairDoubleArrayList(double num)
  {
	  thisDouble = num;
      isDouble = true;
  }
  public boolean isDouble() {
	  return isDouble;
  }
  public double getDouble() {
	  if (isDouble)
		  return thisDouble;
	  else
		  return -1;
  }
  
  public PairOfDoubleArrayList getPairDoubleArrayList() {
	  if (!isDouble)
		  return thispairOfDA;
	  else
		  return null;
  }
  
  public void readFields(DataInput in)
    throws IOException
  {
	  isDouble = in.readBoolean();
	  if (isDouble)
		  thisDouble = in.readDouble();
	  else {
		  thispairOfDA = new PairOfDoubleArrayList();
		  thispairOfDA.readFields(in);
	  }
		  
    	
  }

  public void write(DataOutput out)
    throws IOException
  {
    out.writeBoolean(isDouble);
    if (isDouble)
    	out.writeDouble(thisDouble);
    else
    	thispairOfDA.write(out);
  }



  public String toString()
  {
    if (isDouble)
    	return "true"+" "+thisDouble;
    else
    	return "false"+" "+thispairOfDA.toString();
  }

}