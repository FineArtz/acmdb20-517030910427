package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> ItemList = new ArrayList<>();

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return ItemList.iterator();
    }

    private static final long serialVersionUID = 1L;
    private Integer size = 0;
    private int hashcode = -1;
    private int typeHashcode = -1;
    private String tostring = null;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int len = typeAr.length;
        assert len == fieldAr.length;
        assert len >= 1;
        for (int i = 0; i < len; ++i) {
            ItemList.add(new TDItem(typeAr[i], fieldAr[i]));
            size += typeAr[i].getLen();
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        if (typeAr == null) {
            return;
        }
        int len = typeAr.length;
        assert len >= 1;
        for (int i = 0; i < len; ++i) {
            ItemList.add(new TDItem(typeAr[i], null));
            size += typeAr[i].getLen();
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return ItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i > ItemList.size()) {
            throw new NoSuchElementException(String.format("The max range is %d, but received %d.", ItemList.size(), i));
        }
        return ItemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i > ItemList.size()) {
            throw new NoSuchElementException(String.format("The max range is %d, but received %d.", ItemList.size(), i));
        }
        return ItemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        int i = 0;
        for (TDItem it : ItemList) {
            if (it.fieldName == null) {
                if (name == null) {
                    return i;
                }
            }
            else if (it.fieldName.equals(name)) {
                return i;
            }
            ++i;
        }
        throw new NoSuchElementException(String.format("Can not find filed name %s.", name));
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        TupleDesc td = new TupleDesc(null);
        td.ItemList.addAll(td1.ItemList);
        td.ItemList.addAll(td2.ItemList);
        td.size = td1.size + td2.size;
        return td;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc td = (TupleDesc) o;
        return typeHashcode() == td.typeHashcode();
    }

    private int typeHashcode() {
        if (typeHashcode == -1) {
            StringBuilder sb = new StringBuilder();
            int len = ItemList.size();
            for (int i = 0; i < len - 1; ++i) {
                sb.append(ItemList.get(i).fieldType.toString());
                sb.append(", ");
            }
            sb.append(ItemList.get(len - 1).fieldType.toString());
            typeHashcode = sb.toString().hashCode();
        }
        return typeHashcode;
    }

    public int hashCode() {
        if (hashcode == -1) {
            hashcode = toString().hashCode();
        }
        return hashcode;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        if (tostring == null) {
            StringBuilder sb = new StringBuilder();
            int len = ItemList.size();
            for (int i = 0; i < len - 1; ++i) {
                sb.append(ItemList.get(i).toString());
                sb.append(", ");
            }
            sb.append(ItemList.get(len - 1).toString());
            tostring = sb.toString();
        }
        return tostring;
    }
}
