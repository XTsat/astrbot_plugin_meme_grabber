import asyncio
import datetime
import ipaddress
import os
import shutil
import time
import urllib.parse
import uuid
from pathlib import Path

import aiohttp
import filetype

import astrbot.api.message_components as Comp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.message.components import BaseMessageComponent, Image, Reply
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)


class MemeGrabberPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        # 获取插件数据目录（使用框架规范方法）
        default_data_dir = StarTools.get_data_dir(self.name)
        temp_dir = self.config.get("temp_dir", default_data_dir)
        # 转换为绝对路径，确保是字符串类型
        self.data_dir = os.path.abspath(str(temp_dir))

        # 校验 temp_dir 必须在插件数据目录白名单内
        default_data_dir_abs = os.path.abspath(str(default_data_dir))
        if not Path(self.data_dir).is_relative_to(Path(default_data_dir_abs)):
            logger.warning(
                f"配置的 temp_dir {self.data_dir} 不在默认数据目录 {default_data_dir_abs} 内，使用默认数据目录"
            )
            self.data_dir = default_data_dir_abs

        os.makedirs(self.data_dir, exist_ok=True)
        # 获取是否在发送后删除临时文件的配置
        self.delete_after_send = self.config.get("delete_after_send", True)
        # 获取默认图片扩展名
        self.default_extension = self.config.get("default_extension", "jpg")
        # 获取图片下载超时时间
        self.download_timeout = self.config.get("download_timeout", 60)
        # 延迟初始化 aiohttp ClientSession，首次使用时创建
        self.session = None
        # 用于保护 session 初始化的锁
        self.session_lock = asyncio.Lock()
        # 并发下载限制信号量
        self.download_semaphore = asyncio.Semaphore(5)

    async def _get_session(self):
        """
        获取或创建 aiohttp ClientSession

        Returns:
            aiohttp.ClientSession: 异步HTTP会话
        """
        async with self.session_lock:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession()
        return self.session

    async def download_image(self, picture_url: str, relative_path: str) -> bool:
        """
        下载图片到本地

        Args:
            picture_url: 图片URL
            relative_path: 保存路径

        Returns:
            bool: 下载是否成功
        """
        try:
            # 安全校验：确保只允许 http/https 协议
            parsed_url = urllib.parse.urlparse(picture_url)
            if parsed_url.scheme not in ("http", "https"):
                logger.error(f"不支持的URL协议: {parsed_url.scheme}")
                return False

            # 防止 SSRF 攻击：解析域名并检查是否为内网地址
            try:
                hostname = parsed_url.netloc.split(":")[0]  # 移除端口号
                # 使用异步DNS解析
                loop = asyncio.get_running_loop()
                addr_info = await loop.getaddrinfo(hostname, None)
                ip_address = addr_info[0][4][0]
                # 检查是否为内网地址
                try:
                    ip_obj = ipaddress.ip_address(ip_address)
                    if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
                        logger.error(f"禁止访问内网地址: {ip_address}")
                        return False
                except ValueError:
                    # 非有效IP地址，可能是域名
                    pass
            except Exception as e:
                logger.error(f"解析URL地址时发生错误: {str(e)}")
                return False

            # 获取或创建 ClientSession
            session = await self._get_session()
            # 使用复用的 ClientSession 进行异步请求，设置超时时间
            async with self.download_semaphore:
                async with session.get(
                    picture_url, timeout=self.download_timeout
                ) as response:
                    if response.status == 200:
                        # 流式下载，设置最大文件大小为 10MB
                        max_size = 10 * 1024 * 1024
                        current_size = 0
                        # 确保目录存在
                        os.makedirs(os.path.dirname(relative_path), exist_ok=True)
                        with open(relative_path, "wb") as f:
                            async for chunk in response.content:
                                current_size += len(chunk)
                                if current_size > max_size:
                                    logger.error("图片文件过大，超过10MB")
                                    # 清理已下载的部分文件
                                    if os.path.exists(relative_path):
                                        os.remove(relative_path)
                                        logger.info(
                                            f"已清理过大的临时文件: {relative_path}"
                                        )
                                    return False
                                f.write(chunk)
                        return True
                    else:
                        logger.error(f"下载图片失败，状态码: {response.status}")
                        return False
        except Exception as e:
            logger.exception(f"下载图片时发生错误: {str(e)}")
            # 清理可能的临时文件
            if os.path.exists(relative_path):
                os.remove(relative_path)
                logger.info(f"已删除: {os.path.basename(relative_path)}")
            return False

    async def send_file_to_user(
        self,
        event: AstrMessageEvent,
        file_path: str,
        filename: str,
        is_plugin_created: bool = True,
    ):
        """
        发送文件给用户

        Args:
            event: 消息事件
            file_path: 文件路径
            filename: 文件名
            is_plugin_created: 是否为插件创建的文件（用于控制删除行为）
        """
        try:
            # 使用 AstrBot 官方接口发送文件
            chain: list[BaseMessageComponent] = [
                Comp.File(file=file_path, name=filename)
            ]
            # 使用 yield 发送，保持生成器函数特性
            yield event.chain_result(chain)
        except Exception as e:
            logger.error(f"发送文件时发生错误: {str(e)}")
            yield event.plain_result(f"发送文件失败: {str(e)}")
        finally:
            # 根据配置决定是否删除临时文件，仅删除插件自己创建的文件
            if self.delete_after_send and is_plugin_created:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logger.info(f"已删除: {os.path.basename(file_path)}")
                except Exception as e:
                    logger.error(f"删除临时文件时发生错误: {str(e)}")
            event.stop_event()

    def _generate_filename(self, ext: str) -> str:
        """
        生成唯一的文件名

        Args:
            ext: 文件扩展名，包含点号

        Returns:
            str: 生成的文件名
        """
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        timestamp = int(time.time() * 1000)
        unique_id = uuid.uuid4().hex[:8]
        return f"meme_{date_str}_{timestamp}_{unique_id}{ext}"

    async def _process_local_image(self, event: AstrMessageEvent, localdiskpath: str):
        """
        处理本地图片路径，转换为可发送文件

        Args:
            event: 消息事件
            localdiskpath: 本地图片路径

        Yields:
            处理结果
        """
        try:
            temp_abs_path = os.path.abspath(localdiskpath)

            # 只通过filetype库判断文件格式
            file_extension = f".{self.default_extension}"  # 默认扩展名

            try:
                kind = filetype.guess(temp_abs_path)
                if kind and kind.extension:
                    file_extension = f".{kind.extension}"
            except Exception as e:
                logger.error(f"使用filetype判断图片类型失败: {str(e)}")

            # 生成唯一的文件名
            filename = self._generate_filename(file_extension)

            # 复制图片到我们的临时目录
            temp_path = os.path.join(self.data_dir, filename)

            try:
                shutil.copy2(localdiskpath, temp_path)
                abs_path = os.path.abspath(temp_path)
                logger.info(f"已获取: {filename}")
                # 发送插件创建的文件，允许删除
                async for result in self.send_file_to_user(
                    event, abs_path, filename, is_plugin_created=True
                ):
                    yield result
            except Exception as e:
                logger.error(f"复制图片到临时目录失败: {str(e)}")
                abs_path = temp_abs_path
                # 发送原始文件，不允许删除
                async for result in self.send_file_to_user(
                    event, abs_path, filename, is_plugin_created=False
                ):
                    yield result
        except Exception as e:
            logger.error(f"处理本地图片时发生错误: {str(e)}")
            yield event.plain_result(f"处理图片失败: {str(e)}")
            event.stop_event()
            event.should_call_llm(False)

    async def _process_image_component(self, img_component: Image):
        """
        使用框架标准方法处理 Image 组件，转换为可发送的文件

        Args:
            img_component: Image 组件

        Returns:
            tuple: (文件路径, 文件名, 是否为临时文件)
        """
        try:
            file_path = await img_component.convert_to_file_path()
            if not file_path:
                logger.error("Image.convert_to_file_path 返回空路径")
                return None, None, False

            file_extension = f".{self.default_extension}"
            try:
                kind = filetype.guess(file_path)
                if kind and kind.extension:
                    file_extension = f".{kind.extension}"
            except Exception as e:
                logger.exception(f"使用filetype判断图片类型失败: {str(e)}")

            filename = self._generate_filename(file_extension)
            temp_path = os.path.join(self.data_dir, filename)

            try:
                shutil.copy2(file_path, temp_path)
                abs_path = os.path.abspath(temp_path)
                logger.info(f"已获取: {filename}")
                return abs_path, filename, True
            except Exception as e:
                logger.exception(f"复制图片到临时目录失败: {str(e)}")
                abs_path = os.path.abspath(file_path)
                return abs_path, filename, False
        except Exception as e:
            logger.exception(f"处理 Image 组件时发生错误: {str(e)}")
            return None, None, False

    async def _process_image_to_file(self, event: AiocqhttpMessageEvent, img_msg: dict):
        """
        处理图片消息字典并转换为可发送的文件
        Args:
            event: 消息事件
            img_msg: 图片消息字典

        Returns:
            tuple: (文件路径, 文件名, 是否为临时文件)
        """
        try:
            picture_url = img_msg.get("data", {}).get("url")
            file_id = img_msg.get("data", {}).get("file")

            if picture_url and "/club/item/" in picture_url:
                logger.info(f"处理图片: {picture_url}")
                parsed_url = urllib.parse.urlparse(picture_url)
                path = parsed_url.path
                ext = os.path.splitext(path)[1].lower()
                if not ext:
                    ext = f".{self.default_extension}"

                filename = self._generate_filename(ext)
                relative_path = os.path.join(self.data_dir, filename)

                result = await self.download_image(picture_url, relative_path)
                if result:
                    absolute_path = os.path.abspath(relative_path)
                    return absolute_path, filename, True
            elif file_id:
                img_response = await event.bot.api.call_action(
                    "get_image", file=file_id
                )
                localdiskpath = img_response.get("file")
                if not localdiskpath:
                    logger.error("获取图片文件路径失败")
                    return None, None, False

                file_extension = f".{self.default_extension}"
                try:
                    kind = filetype.guess(localdiskpath)
                    if kind and kind.extension:
                        file_extension = f".{kind.extension}"
                except Exception as e:
                    logger.exception(f"使用filetype判断图片类型失败: {str(e)}")

                filename = self._generate_filename(file_extension)
                temp_path = os.path.join(self.data_dir, filename)
                try:
                    shutil.copy2(localdiskpath, temp_path)
                    abs_path = os.path.abspath(temp_path)
                    logger.info(f"已获取: {filename}")
                    return abs_path, filename, True
                except Exception as e:
                    logger.exception(f"复制图片到临时目录失败: {str(e)}")
                    abs_path = os.path.abspath(localdiskpath)
                    return abs_path, filename, False
            else:
                logger.error("图片消息中缺少URL或file_id")
                return None, None, False
        except Exception as e:
            logger.exception(f"处理图片时发生错误: {str(e)}")
        return None, None, False

    async def handle_reply_message(self, event: AstrMessageEvent, reply_msg: Reply):
        """
        处理回复消息

        Args:
            event: 消息事件
            reply_msg: 回复消息
        """
        try:
            if not isinstance(event, AiocqhttpMessageEvent):
                yield event.plain_result("抱歉，该功能仅支持 QQ 平台")
                event.stop_event()
                event.should_call_llm(False)
                return

            found_images = []
            temp_files = []

            # 优先从 reply_msg.chain 获取图片（框架已解析好的消息段）
            if reply_msg.chain and isinstance(reply_msg.chain, list):
                image_components = [
                    msg for msg in reply_msg.chain
                    if isinstance(msg, Image)
                ]
                if image_components:
                    tasks = [self._process_image_component(img) for img in image_components]
                    results = await asyncio.gather(*tasks)
                    for file_path, filename, is_temp in results:
                        if file_path and filename:
                            found_images.append((file_path, filename))
                            if is_temp:
                                temp_files.append(file_path)

            # 如果 chain 中没有找到图片，回退到 get_msg API
            if not found_images:
                client = event.bot
                response = await client.api.call_action("get_msg", message_id=reply_msg.id)
                reply_msg_content = response.get("message", [])
                if not reply_msg_content:
                    yield event.plain_result("引用消息格式错误")
                    event.stop_event()
                    event.should_call_llm(False)
                    return

                tasks = []
                for msg in reply_msg_content:
                    if msg.get("type") == "image":
                        tasks.append(self._process_image_to_file(event, msg))

                results = await asyncio.gather(*tasks)
                for file_path, filename, is_temp in results:
                    if file_path and filename:
                        found_images.append((file_path, filename))
                        if is_temp:
                            temp_files.append(file_path)

            if not found_images:
                yield event.plain_result("引用消息中未找到图片")
                event.stop_event()
                event.should_call_llm(False)
                return

            chain: list[BaseMessageComponent] = []
            for file_path, filename in found_images:
                chain.append(Comp.File(file=file_path, name=filename))

            try:
                yield event.chain_result(chain)
            except Exception as e:
                logger.exception(f"发送文件时发生错误: {str(e)}")
                yield event.plain_result(f"发送文件失败: {str(e)}")
            finally:
                if self.delete_after_send:
                    for file_path in temp_files:
                        try:
                            if os.path.exists(file_path):
                                os.remove(file_path)
                                logger.info(f"已删除: {os.path.basename(file_path)}")
                        except Exception as e:
                            logger.exception(f"删除临时文件时发生错误: {str(e)}")

            event.stop_event()
        except Exception as e:
            logger.exception(f"处理回复消息时发生错误: {str(e)}")
            yield event.plain_result(f"处理回复失败: {str(e)}")
            event.stop_event()
            event.should_call_llm(False)

    @filter.command("提取")
    async def convert_command(self, event: AstrMessageEvent):
        """
        提取图片为可保存的文件格式

        用法: 发送图片或回复包含图片的消息，然后输入 /提取 指令
        """
        event.should_call_llm(False)
        message_chain = event.get_messages()

        # 检查平台是否支持
        if event.get_platform_name() != "aiocqhttp":
            yield event.plain_result("抱歉，该功能仅支持 QQ 平台")
            event.stop_event()
            event.should_call_llm(False)
            return

        found_images = []

        # 先收集所有图片和回复
        for msg in message_chain:
            if msg.type == "Image" and isinstance(msg, Image):
                found_images.append(msg)
            elif msg.type == "Reply" and isinstance(msg, Reply):
                # 处理回复消息（已经支持多个图片）
                async for result in self.handle_reply_message(event, msg):
                    yield result
                # 回复消息处理完成后直接返回
                return

        # 处理收集到的所有图片
        if found_images:
            # 构建包含所有文件的消息链
            chain: list[BaseMessageComponent] = []
            temp_files = []

            # 使用框架标准方法处理 Image 组件
            tasks = []
            for img_component in found_images:
                tasks.append(self._process_image_component(img_component))

            results = await asyncio.gather(*tasks)

            for file_path, filename, is_temp in results:
                if file_path and filename:
                    chain.append(Comp.File(file=file_path, name=filename))
                    if is_temp:
                        temp_files.append(file_path)

            try:
                # 发送所有文件
                yield event.chain_result(chain)
            except Exception as e:
                logger.exception(f"发送文件时发生错误: {str(e)}")
                yield event.plain_result(f"发送文件失败: {str(e)}")
            finally:
                # 根据配置决定是否删除临时文件
                if self.delete_after_send:
                    for file_path in temp_files:
                        try:
                            if os.path.exists(file_path):
                                os.remove(file_path)
                                logger.info(f"已删除: {os.path.basename(file_path)}")
                        except Exception as e:
                            logger.exception(f"删除临时文件时发生错误: {str(e)}")

            event.stop_event()
            return

        # 没有找到图片
        yield event.plain_result("请引用表情包或在对话中包含表情包")
        event.stop_event()
        event.should_call_llm(False)

    async def terminate(self):
        """插件终止时的清理操作"""
        # 关闭 aiohttp ClientSession
        if self.session is not None and not self.session.closed:
            try:
                await self.session.close()
                logger.info("已关闭 aiohttp ClientSession")
            except Exception as e:
                logger.exception(f"关闭 ClientSession 时发生错误: {str(e)}")

        # 只有当开启了清理临时文件时才执行清理操作
        if self.delete_after_send:
            # 清理可能遗留的临时图片文件（仅删除插件生成的 meme_ 前缀文件）
            try:
                if os.path.exists(self.data_dir):
                    for file in os.listdir(self.data_dir):
                        if file.startswith("meme_"):
                            file_path = os.path.join(self.data_dir, file)
                            if os.path.isfile(file_path):
                                os.remove(file_path)
                                logger.info(f"已删除: {os.path.basename(file_path)}")
            except Exception as e:
                logger.exception(f"清理临时文件时发生错误: {str(e)}")
        else:
            logger.info("未开启清理临时文件，跳过清理操作")

    async def on_unload(self):
        """框架卸载插件时的钩子方法"""
        await self.terminate()
