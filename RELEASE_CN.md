# Aliyun LOG Python SDK 发布流程

## 发布至 PyPI

当`aliyun-log-python-sdk`发生变更时，需要将其发布至 PyPI。

### 步骤
1. 进入`aliyun-log-python-sdk`项目的根目录。
2. 更新文件`aliyun/log/version.py`中的字段`__version__`至下一个版本。
3. 运行命令`python setup.py sdist`构建发布文件。
4. 运行命令`pip install twine`安装用于向 PyPI 中发布 Python 包的工具 twine。
5. 运行命令`twine upload dist/*`将构建好的文件发布至 PyPI（需要填写用户名/密码）。
6. 进入 [release](https://github.com/aliyun/aliyun-log-python-sdk/releases) 页面，点击`Draft a new release`为该发布的版本创建一个 release。
`Tag version`和`Release title`填写该版本的版本号。

### 验证
运行命令`pip install -U aliyun-log-python-sdk`检查是否能安装到刚发布的 SDK。
