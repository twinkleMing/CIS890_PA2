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
public class ArrayListWritableComparable<E extends WritableComparable> extends ArrayList<E>
  implements WritableComparable
{
  private static final long serialVersionUID = 1L;

  public ArrayListWritableComparable()
  {
  }

  public ArrayListWritableComparable(ArrayList<E> array)
  {
    super(array);
  }

  public void readFields(DataInput in)
    throws IOException
  {
    clear();

    int numFields = in.readInt();
    if (numFields == 0)
      return;
    String className = in.readUTF();
    try
    {
      Class c = Class.forName(className);
      for (int i = 0; i < numFields; i++) {
        WritableComparable obj = (WritableComparable)c.newInstance();
        obj.readFields(in);
        add((E) obj);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void write(DataOutput out)
    throws IOException
  {
    out.writeInt(size());
    if (size() == 0)
      return;
    WritableComparable obj = (WritableComparable)get(0);

    out.writeUTF(obj.getClass().getCanonicalName());

    for (int i = 0; i < size(); i++) {
      obj = (WritableComparable)get(i);
      if (obj == null) {
        throw new IOException("Cannot serialize null fields!");
      }
      obj.write(out);
    }
  }

  public int compareTo(Object obj)
  {
    ArrayListWritableComparable that = (ArrayListWritableComparable)obj;

    for (int i = 0; i < size(); i++)
    {
      if (i >= that.size()) {
        return 1;
      }

      Comparable thisField = (Comparable)get(i);

      Comparable thatField = (Comparable)that.get(i);

      if (thisField.equals(thatField))
      {
        if (i == size() - 1) {
          if (size() > that.size()) {
            return 1;
          }
          if (size() < that.size())
            return -1;
        }
      }
      else {
        return thisField.compareTo(thatField);
      }
    }

    return 0;
  }

  public String toString()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("[");
    for (int i = 0; i < size(); i++) {
      if (i != 0)
        sb.append(", ");
      sb.append(get(i));
    }
    sb.append("]");

    return sb.toString();
  }

}