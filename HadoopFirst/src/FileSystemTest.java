import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileSystemTest {

	/**
	 * 两个问题：
	 * 
	 * 1、删除某个指定目录下的所有 .class 的文件 如果文件夹也是以class结尾。那么不能删除 如果文件是以.class结尾， 那么删掉
	 * 
	 * 2、上传一个大文件有 300M， 最后会被切分成3个块。 通过编写代码去获取第二个块。 128M --- 256M
	 */
	private FileSystem fs;

	@Before
	public void init() throws IOException {
		/*
		 * 构造函数 静态方法 反射 克隆 反序列化
		 */

		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
		conf.set("dfs.replication", "2");
		fs = FileSystem.get(conf);
	}

	@Test
	public void upload() {
		try {
			fs.copyFromLocalFile(new Path("D:\\hadoopvi.wmv"), new Path("/hvi.wmv"));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void deleteClass() throws IllegalArgumentException, IOException {

		delete(fs, "/", "class");
	}

	@Test
	public void getDesblock() {

		try {

			FSDataInputStream in = fs.open(new Path("/hvi.wmv"));
			FileStatus[] listStatus = fs.listStatus(new Path("/hvi.wmv"));
			BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(listStatus[0], 0L, listStatus[0].getLen());
			long length = fileBlockLocations[2].getLength();
			long offset = fileBlockLocations[2].getOffset();
			long startOffset = offset;
			int arrIndex = 2;
			byte[] b = new byte[4096];
			FileOutputStream os = new FileOutputStream(new File("d:/block1"));
			while ((in.read(offset, b, 0, 4096)) != -1) {
				os.write(b);
				offset += 4096;

				if (offset > startOffset + length) {
					return;
				}
			}

			os.flush();
			os.close();
			in.close();

		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void delete(FileSystem fs, String path, String desType)
			throws FileNotFoundException, IllegalArgumentException, IOException {
		RemoteIterator<LocatedFileStatus> listLocatedStatus = fs.listLocatedStatus(new Path(path));
		while (listLocatedStatus.hasNext()) {
			LocatedFileStatus next = listLocatedStatus.next();
			String nextPath = next.getPath().toUri().toString();

			if (next.isDirectory()) {
				delete(fs, nextPath, desType);
			} else {
				FileStatus[] listStatus = fs.listStatus(new Path(path), new PathFilter() {

					@Override
					public boolean accept(Path p) {
						String name = p.getName();
						int length = p.getName().length();
						if (length > desType.length()) {
							return name.substring(name.indexOf(".") + 1).equals(desType);
						} else {
							return false;
						}
					}
				});

				for (FileStatus f : listStatus) {
					System.out.println("&&&&&&&&&& \t" + f.getPath());
					fs.delete(f.getPath(), true);
				}
			}
		}

	}

	@After
	public void close() throws IOException {
		fs.close();
	}

}
