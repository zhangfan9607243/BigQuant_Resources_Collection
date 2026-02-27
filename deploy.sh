#!/bin/bash
set -e
git init
git add .
git commit -m "deploy all"
git branch -M main
git remote remove origin 2>/dev/null || true
git remote add origin git@github.com:zhangfan9607243/BigQuant_Resources_Collection.git
git push -f origin main
