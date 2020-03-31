#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2020/3/3 17:51

import tarfile
import zipfile
from pathlib import Path


def tar(tarname, filelist, path=True, **kwargs):
    tar = tarfile.open(str(tarname), 'w:gz')
    for f in filelist:
        tar.add(f, arcname=Path(f).name if path else None, **kwargs)
    tar.close()


def zip(zipname, filelist, path=True, **kwargs):
    z = zipfile.ZipFile(zipname, 'w')
    for f in filelist:
        z.write(f, arcname=Path(f).name if path else None, **kwargs)
    z.close()
