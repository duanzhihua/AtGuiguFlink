package com.atguigu.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @description:
 * @author: liyang
 * @date: 2020/11/8 21:06
 */
@RestController
@RequestMapping("/")
public class FileUploadController {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/");

    @PostMapping("/uploads")
    public String upload(MultipartFile[] uploadFiles, HttpServletRequest req){
        String realPath = req.getSession().getServletContext().getRealPath("/uploadFile/");
        String format = sdf.format(new Date());
        String path = "D:/IDEAWorkplace/AtGuiguFlink/src/main/resources/uploadFile/";
        File folder = new File(path + format);
        if(!folder.isDirectory()){
            folder.mkdirs();
        }
        try {
            for(MultipartFile uploadFile:uploadFiles) {
                String oldName = uploadFile.getOriginalFilename();
                String newName = UUID.randomUUID().toString() +
                        oldName.substring(oldName.lastIndexOf("."), oldName.length());
                uploadFile.transferTo(new File(folder, newName));
                System.out.println("++++");
            }
            return "上传成功";
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "上传失败！";
    }

    @PostMapping("/upload")
    public String upload(MultipartFile uploadFile, HttpServletRequest req){
        String realPath = req.getSession().getServletContext().getRealPath("/uploadFile/");
        String format = sdf.format(new Date());
        String path = "D:/IDEAWorkplace/AtGuiguFlink/src/main/resources/uploadFile/";
        File folder = new File(path + format);
        if(!folder.isDirectory()){
            folder.mkdirs();
        }
        try {
            String oldName = uploadFile.getOriginalFilename();
            String newName = UUID.randomUUID().toString() +
                    oldName.substring(oldName.lastIndexOf("."), oldName.length());
            uploadFile.transferTo(new File(folder, newName));
            return "上传成功";
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "上传失败！";
    }
}
