import pandas as pd
import random
import faker

# 定义生成数据的条数
num_records = 100000000  # 可以根据需要修改

# 创建Faker实例
fake = faker.Faker('zh_CN')

# 生成数据并分批写入文件
batch_size = 100000  # 每批次写入的记录数
data = []

for _ in range(num_records):
    record = {
        'id': _ + 1,
        '身份证': fake.ssn(),
        '姓名': fake.name(),
        '电话': fake.phone_number(),
        '地址': fake.address()
    }
    data.append(record)

    # 当内存中有1万条记录时，写入文件
    if len(data) == batch_size:
        df = pd.DataFrame(data)
        df.to_csv('simulated_data.csv', mode='a', index=False, header=not bool(_), encoding='utf-8-sig')
        data = []  # 清空数据列表以释放内存
        
        # 打印进度
        print(f"已生成 {_ + 1} 条记录，共 {num_records} 条记录，进度: {(_ + 1) / num_records * 100:.2f}%")

# 写入剩余的数据
if data:
    df = pd.DataFrame(data)
    df.to_csv('simulated_data.csv', mode='a', index=False, header=not bool(_), encoding='utf-8-sig')
    
    # 打印进度
    print(f"已生成 {_ + 1} 条记录，共 {num_records} 条记录，进度: {(_ + 1) / num_records * 100:.2f}%")


print("数据已生成并保存为 simulated_data.csv")
