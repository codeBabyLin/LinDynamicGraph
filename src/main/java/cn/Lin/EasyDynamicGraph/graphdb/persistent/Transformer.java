package cn.Lin.EasyDynamicGraph.graphdb.persistent;

//import cn.Lin.EasyDynamicGraph.Common.Mapserializer;
//import cn.Lin.EasyDynamicGraph.Common.Mapserializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

public class Transformer {

    private ByteBufAllocator allocator;


    private ByteBuf byteBuf;

    //private Mapserializer mapserializer;


    public Transformer(){
        this.allocator  = ByteBufAllocator.DEFAULT;
        this.byteBuf = allocator.heapBuffer();
        //this.mapserializer = new Mapserializer();
    }

    public byte[] LongToByte(Long l){
        //ByteBuf byteBuf  = allocator.heapBuffer();
        byteBuf.clear();
        byteBuf.writeLong(l);
        byte[] data= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);
        return data;
    }
    public Long ByteToLong(byte[] data){
        ByteBuf byteBuf  = Unpooled.copiedBuffer(data);
        Long d = byteBuf.readLong();
        return d;
    }
    public byte[] IntToByte(Integer l){
        //ByteBuf byteBuf  = allocator.heapBuffer();
        byteBuf.clear();
        byteBuf.writeInt(l);
        byte[] data= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);
        return data;
    }
    public Integer ByteToInt(byte[] data){
        ByteBuf byteBuf  = Unpooled.copiedBuffer(data);
        Integer d = byteBuf.readInt();
        return d;
    }
    public byte[] DoubleToByte(Double l){
        //ByteBuf byteBuf  = allocator.heapBuffer();
        byteBuf.clear();
        byteBuf.writeDouble(l);
        byte[] data= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);
        return data;
    }
    public Double ByteToDouble(byte[] data){
        ByteBuf byteBuf  = Unpooled.copiedBuffer(data);
        Double d = byteBuf.readDouble();
        return d;
    }

    public byte[] FloatToByte(Float l){
        //ByteBuf byteBuf  = allocator.heapBuffer();
        byteBuf.clear();
        byteBuf.writeFloat(l);
        byte[] data= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);
        return data;
    }
    public Float ByteToFloat(byte[] data){
        ByteBuf byteBuf  = Unpooled.copiedBuffer(data);
        Float d = byteBuf.readFloat();
        return d;
    }
    public byte[] BooleanToByte(Boolean l){
        //ByteBuf byteBuf  = allocator.heapBuffer();
        byteBuf.clear();
        byteBuf.writeBoolean(l);
        byte[] data= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);
        return data;
    }
    public Boolean ByteToBoolean(byte[] data){
        ByteBuf byteBuf  = Unpooled.copiedBuffer(data);
        Boolean d = byteBuf.readBoolean();
        return d;
    }


  /*  public static String _readString(ByteBuf byteBuf){
        int length = byteBuf.readInt();
        return (String) byteBuf.readCharSequence(length, StandardCharsets.UTF_8);
    }*/
/*    public static void _writeString(String str, ByteBuf byteBuf){
        byteBuf.writeByte(JavaSerialzerDataType.STRING_ID);
        byteBuf.writeInt(str.length());
        byteBuf.writeCharSequence(str, StandardCharsets.UTF_8);
    }*/

    public String ByteToString(byte[] value){
        ByteBuf byteBuf  = Unpooled.copiedBuffer(value);
        int length = byteBuf.readInt();
        return (String) byteBuf.readCharSequence(length, StandardCharsets.UTF_8);
    }
    public byte[] StringToByte(String value){
        byteBuf.clear();
        byteBuf.writeInt(value.length());
        byteBuf.writeCharSequence(value, StandardCharsets.UTF_8);

        byte[] data= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);
        return data;
    }





 /*  public byte[] NodeToByte(Node node){
       ByteBuf byteBuf  = allocator.heapBuffer();
       byteBuf.writeLong((Long)node);
       byte[] data= new byte[byteBuf.writerIndex()];
       byteBuf.readBytes(data);
      return data;
   }
   public Node ByteToNode(byte[] data){
       ByteBuf byteBuf  = Unpooled.copiedBuffer(data);
       Long d = (Long)byteBuf.readLong();
       return (Node)d;
   }*/

   /* public byte[] RelationToByte(Relation relation){
        ByteBuf byteBuf  = allocator.heapBuffer();
        byteBuf.writeLong((Long)relation);
        byte[] data= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);
        return data;
    }
    public Relation ByteToRelation(byte[] data){
        ByteBuf byteBuf  = Unpooled.copiedBuffer(data);
        Long d = (Long)byteBuf.readLong();
        return (Relation)d;
    }*/

   public ByteBuf getByteBuf(){
       byteBuf.clear();
       return this.byteBuf;
   }
   public ByteBuf getByteBuf(byte[]data){
       return Unpooled.copiedBuffer(data);
   }


   public byte[]LongArrayToByte(long dataArray[]){
       byteBuf.clear();

       int size = dataArray.length;
       byteBuf.writeInt(size);
       for(int i = 0;i<size;i++){
           byteBuf.writeLong(dataArray[i]);
       }
       //mapserializer.writeLongArray(datArray,byteBuf);
       byte[] data= new byte[byteBuf.writerIndex()];
       byteBuf.readBytes(data);
       return data;
   }

    public byte[]LongArrayToByteWithOutSize(long dataArray[]){
        byteBuf.clear();

        int size = dataArray.length;
        //byteBuf.writeInt(size);
        for(int i = 0;i<size;i++){
            byteBuf.writeLong(dataArray[i]);
        }
        //mapserializer.writeLongArray(datArray,byteBuf);
        byte[] data= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);
        return data;
    }

/*   public byte[]LongArrayToByte(long dataArray1[],long dataArray2[]){
       int size = dataArray1.length;
       byteBuf.writeInt(size);
       for(int i = 0;i<size;i++){
           byteBuf.writeLong(dataArray1[i]);
       }
   }
   */


  /* public byte[] VersionToByte(long version[]){
        //ByteBuf byteBuf  = allocator.heapBuffer();
       byteBuf.clear();
        byteBuf.writeLong(version[0]);
        byteBuf.writeLong(version[1]);
        byte[] data= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);
        return data;
    }
    public long[] ByteToVersion(byte[] data){
        ByteBuf byteBuf  = Unpooled.copiedBuffer(data);
        long v1 = byteBuf.readLong();
        long v2 = byteBuf.readLong();
        return new long[]{v1,v2};
    }
    */

    public byte[] byteArrayCancat(byte[]a,byte[]b){
      byteBuf.clear();
      byteBuf.writeBytes(a);
      byteBuf.writeBytes(b);
      byte[] data= new byte[byteBuf.writerIndex()];
      byteBuf.readBytes(data);
      return data;
    }
    public byte[] ObjectToByte(Object value){
        byte prefix = -1;
        byte []tempvalue;
        if(value instanceof String){
            prefix = 0;
            tempvalue= StringToByte(String.valueOf(value));
        }
        else if(value instanceof Integer){
            prefix = 4;
            tempvalue= IntToByte((Integer)value);
        }
        else if(value instanceof Long){
            prefix = 8;
            tempvalue=LongToByte((Long)value);
        }
        else if(value instanceof Double){
            prefix = 16;
            tempvalue= DoubleToByte((Double)value);
        }
        else if(value instanceof Float){
            prefix = 9;
            tempvalue= FloatToByte((Float)value);
        }
        else if(value instanceof Boolean){
            prefix = 1;
            tempvalue= BooleanToByte((Boolean)value);
        }
        else{
            tempvalue= new byte[0];
        }
        return byteArrayCancat(new byte[]{prefix},tempvalue);
    }

    public Object ByteToObject(byte[]data){
        ByteBuf byteBuf  = Unpooled.copiedBuffer(data);
        byte type = byteBuf.readByte();
        byte[] value= new byte[byteBuf.writerIndex()];
        byteBuf.readBytes(data);
        switch (type){
            case 0:
                return ByteToString(value);
            case 4:
                return ByteToInt(value);
            case 8:
                return ByteToLong(value);
            case 9:
                return ByteToFloat(value);
            case 16:
                return ByteToDouble(value);
            case 1:
                return ByteToBoolean(value);
            default:
                return null;
        }

    }


}
