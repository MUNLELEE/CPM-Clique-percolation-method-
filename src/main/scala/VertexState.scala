import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

class VertexState(var weight: Long) extends Serializable with KryoSerializable{
    def this() = {
        this(1L)
    }

    override def write(kryo: Kryo, output: Output): Unit = {
        kryo.writeObject(output, this.weight)
    }

    override def read(kryo: Kryo, input: Input): Unit = {
        this.weight = kryo.readObject(input, classOf[Long])
    }
}
